import gevent
from gevent.queue import Queue
import requests.async
import requests
import atrest
import json
import math
import copy
from sets import Set
from oauth_hook import OAuthHook
from urlparse import parse_qsl


USER_CALL_COUNT = 80
SUPER_USER_FILTER = 50000
FOLLOWED_COUNT_FILTER = 2
INTERSECTION_FILTER = 0.5
CORRESPONDENCE_FILTER = 10

REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
AUTHORIZE_URL = 'https://twitter.com/oauth/authorize'
ACCESS_TOKEN_URL = 'https://twitter.com/oauth/access_token'
REST_API_ROOT = 'https://api.twitter.com/1'


class TwitterOAuthHandler(object):
    @classmethod
    def config_oauth_handler(cls, consumer_key, consumer_secret):
        OAuthHook.consumer_key = consumer_key
        OAuthHook.consumer_secret = consumer_secret
        cls.hook = OAuthHook
    
    @classmethod
    def get_request_token(cls, callback_url):
        oauth_hook = cls.hook(header_auth=True)
        client = requests.session(hooks={'pre_request': oauth_hook})
        response = client.post(REQUEST_TOKEN_URL, {'oauth_callback': callback_url})
        response.raise_for_status()
        response = dict(parse_qsl(response.content))
        token, secret = (response['oauth_token'], response['oauth_token_secret'])
        redirect_url = "%s?oauth_token=%s" % (AUTHORIZE_URL, token)
        return (token, secret, redirect_url)

    @classmethod
    def get_access_token(cls, request_token, request_secret, oauth_verifier):
        oauth_hook = cls.hook(access_token=request_token, access_token_secret=request_secret, header_auth=True)
        client = requests.session(hooks={'pre_request': oauth_hook})
        response = client.post(ACCESS_TOKEN_URL, {'oauth_verifier': oauth_verifier})
        response.raise_for_status()
        response = dict(parse_qsl(response.content))
        user_token, user_secret, user_id, screen_name = (response['oauth_token'], response['oauth_token_secret'], response['user_id'], response['screen_name'])
        return (user_token, user_secret, user_id, screen_name)


class TwitterRestAPI(object):
    def __init__(self, oauth_token=None, oauth_secret=None, cache=None):
        self.apiroot = REST_API_ROOT
        self.cache = cache
        self.token = oauth_token
        self.secret = oauth_secret
    
    def following(self, user_id=None, user_name=None):
        path = "/friends/ids.json"
        params = {"cursor": "-1"}
        if user_id:
            params["user_id"] = user_id
        elif user_name:
            params["screen_name"] = user_name
        response = self.call(path, params)
        following = json.loads(response)
        return following

    def userdetails(self, userids):
        path = "/users/lookup.json"
        details = []
        for i in xrange(int(math.ceil(float(len(userids)) / 100))):
            ileft = 100 * i
            iright = ileft + 100
            params = {"user_id" : ",".join(str(u) for u in userids[ileft:iright])}
            response = self.call(path, params)
            det = json.loads(response)
            details.extend(det)
        return details

    def call(self, relative_path, params={}):
        path = "%s%s" % (self.apiroot, relative_path)
        if self.cache:
            keys = [path, json.dumps(params)]
            if self.token and self.secret:
                keys.extend([self.token, self.secret])
            resp = self.cache.fetch(keys)
        if resp:
            return resp.text
        
        if self.token and self.secret:
            oauth_hook = cls.hook(access_token=self.token, access_token_secret=self.secret, header_auth=True)
            client = requests.session(hooks={'pre_request': oauth_hook})
        else:
            client = requests
        resp = client.get(path, params=params)
        resp.raise_for_status()
        self.cache.store(keys, resp)
        return resp.text


class Grouper(object):
    '''Contains the logic for getting and analyzing group relationships for a user's followeds'''
    def __init__(self, restapi):
        self.restapi = restapi

    def generate_groups(self, user_id=None, user_name=None):
        self.following_queue = Queue()
        self.user_id = user_id
        self.user_name = user_name
        following = self.restapi.following(user_id, user_name)
        user_details = self.restapi.userdetails(following["ids"])
        user_details = self._extract_power_user_group(user_details)
        #store for future use
        self.user_det = dict((det["id"], det) for det in user_details)

        #get top scoring
        user_details.sort(key=lambda d: d["followers_count"] / float(d["friends_count"] + 1))
        user_details = user_details[(-1 * USER_CALL_COUNT):]

        #generate rest calls
        following_calls = [gevent.spawn(self._following_call, user["id"]) for user in user_details]
        
        # queue querying generator
        def user_foll_gen():
            for _ in xrange(len(following_calls)):
                queued = self.following_queue.get()
                if queued is not None:
                    yield queued
        bucket_gen = gevent.spawn(self._generate_follower_buckets, user_foll_gen)
        #join api calls and result processing events
        gevent.joinall(following_calls + [bucket_gen])
        return self.follower_buckets

    def _extract_power_user_group(self, user_details):
        #powerusers list will be stored in the object as an attribute
        power_users = []
        remaining_users = []
        for user in user_details:
            if user["followers_count"] > SUPER_USER_FILTER:
                power_users.append(user)
            else:
                remaining_users.append(user)
        self.power_users = power_users 
        return remaining_users

    def _following_call(self, user_id):
        following = None
        try:
            resp = self.restapi.following(user_id=user_id)
            following = (user_id, resp["ids"])
        finally:
            self.following_queue.put(following) 

    def _generate_follower_buckets(self, user_following):
        follower_buckets = {}
        for user, following in user_following():
            for followed in following:
                if followed in follower_buckets:
                    follower_buckets[followed].add(user)
                else:
                    follower_buckets[followed] = Set([user])
        # filter out low scoring followeds and convert to list of set 2-tuples
        follower_buckets = list((Set([k]),v) for k,v in follower_buckets.items() if len(v) > FOLLOWED_COUNT_FILTER)
        self.follower_buckets = self._consolidate_follower_buckets(follower_buckets)

    def _consolidate_follower_buckets(self, follower_buckets):
        merged_buckets = follower_buckets
        #reverse iteration to enable deletion
        for idx in xrange(len(follower_buckets) - 1, -1, -1):
            user_ids, bucket = follower_buckets[idx] 
            for merge_ids, merge_bucket in merged_buckets:
                if user_ids == merge_ids:
                    # itself
                    continue
                ilen = len(bucket.intersection(merge_bucket))
                cardinality = len(merge_bucket) * INTERSECTION_FILTER 
                cardinality = cardinality if cardinality >= 2 else 2
                if ilen > cardinality:
                    #if intersect, merge
                    merge_ids.update(user_ids)
                    merge_bucket.update(bucket)
                    follower_buckets.pop(idx)
                    break
        follower_buckets = [(similarities, user_ids) for similarities, user_ids in merged_buckets if len(similarities) > CORRESPONDENCE_FILTER] 
        self._extend_user_details(list(set(user_id for similarities, _ in follower_buckets for user_id in similarities)))
        return follower_buckets

    def _extend_user_details(self, user_ids):
        self.user_det.update((user["id"], user) for user in self.restapi.userdetails(user_ids))

    def _convert_buckets_to_groups(self, follower_buckets):
        return [([self.user_det[uid]["name"] for uid in similarities], [self.user_det[uid]["name"] for uid in user_ids])
            for similarities, user_ids in follower_buckets]


def test():
    api = TwitterRestAPI(cache=atrest.Cache(atrest.FileBackend('.cache'), 3600))
    grouper = Grouper(api)
    grouper.generate_groups(user_name='phildlv')
    print grouper.follower_buckets
    print [([grouper.user_det[uid]["screen_name"] for uid in followeds if grouper.user_det.has_key(uid)], [grouper.user_det[uid]["screen_name"] for uid in followers if grouper.user_det.has_key(uid)]) for followeds, followers in grouper.follower_buckets]

if __name__ == '__main__':
    test()
'''
x- score = followers / following + 1

x- using top 100 by score, 

x- construct buckets of following relationships: {followed_user_id, [follower_from_list,]} (async => gevent)
starting with largest bucket, declare a group.
- other groups can extend that group if they have a large enough percentage of identical members,
otherwise, they become a new group.
- Those groups that were formed by 5 or more groups being merged are coherent enough to recommend as groups to follow.
'''
