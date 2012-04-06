import gevent
from gevent.queue import Queue
import requests.async
import requests
from collections import namedtuple
import atrest
import json
import math
import copy
from sets import Set
from oauth_hook import OAuthHook
from urlparse import parse_qsl
import logging

logger = logging.getLogger()

USER_CALL_COUNT = 150
SUPER_USER_FILTER = 50000
FOLLOWED_COUNT_FILTER = 2
INTERSECTION_FILTER = 0.4
CORRESPONDENCE_FILTER = 15

REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
AUTHORIZE_URL = 'https://twitter.com/oauth/authorize'
ACCESS_TOKEN_URL = 'https://twitter.com/oauth/access_token'
REST_API_ROOT = 'https://api.twitter.com/1'


# Notification hook decorator
def notify(func, *args, **kargs):
    '''This decorates class instance methods, the instance of which has a notifier.'''
    def wrapper(self, *args, **kargs):
        result = func(self, *args, **kargs)
        if hasattr(self, 'notifier'):
            if hasattr(self.notifier, 'step'):
                self.notifier.step()
        return result
    return wrapper

class CacheResponse(object):
    pass

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
    def __init__(self, oauth_token=None, oauth_secret=None, cache=None, notifier=None):
        self.apiroot = REST_API_ROOT
        self.cache = cache
        self.token = oauth_token
        self.secret = oauth_secret
        self.notifier = notifier
    
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
        calls = []
        for i in xrange(int(math.ceil(float(len(userids)) / 100))):
            ileft = 100 * i
            iright = ileft + 100
            params = {"user_id" : ",".join(str(u) for u in userids[ileft:iright])}
            calls.append(gevent.spawn(self.call, path, params))
        gevent.joinall(calls)
        for c in calls:
            det = json.loads(c.value)
            details.extend(det)
        return details
    
    def makelist(self, name, description, userids):
        createpath = "/lists/create.json"
        addpath = "/lists/members/create_all.json"
        params = {"name":name[0:25], "description":description}
        response = self.call(createpath, params, header_auth=True, post=True)
        response = json.loads(response)
        listid = response["id_str"]
        params = {"list_id":listid, "user_id":",".join(str(u) for u in userids)}
        print params
        self.call(addpath, params, header_auth=True, post=True)
        return response

    @notify
    def call(self, relative_path, params={}, header_auth=False, post=False):
        path = "%s%s" % (self.apiroot, relative_path)
        if self.cache:
            keys = [path, json.dumps(params)]
            if self.token and self.secret:
                keys.extend([self.token, self.secret])
            resp = self.cache.fetch(keys)
        if resp:
            return resp.text
        if self.token and self.secret:
            oauth_hook = TwitterOAuthHandler.hook(access_token=self.token, access_token_secret=self.secret, header_auth=header_auth)
            client = requests.session(hooks={'pre_request': oauth_hook})
        else:
            client = requests
        if post:
            resp = client.post(path, data=params)
        else:
            resp = client.get(path, params=params)
        try:
            resp.raise_for_status()
        except Exception:
            logger.exception(resp.text)
            raise
        # make cachable (picklable and read-repeatable) vesion of response
        cacheresp = CacheResponse()
        cacheresp.headers = resp.headers
        cacheresp.encoding = resp.encoding
        cacheresp.status_code = resp.status_code
        cacheresp.text = resp.text
        self.cache.store(keys, cacheresp)
        return cacheresp.text


class Grouper(object):
    '''Contains the logic for getting and analyzing group relationships for a user's followeds'''
    def __init__(self, restapi):
        self.restapi = restapi

    def generate_groups(self, user_id=None, user_name=None, notification_hook=None):
        '''
        Either user_id or user_name must be provided. notification_hook is an optional object to which notification of completion of 
        intermediate steps and final completion will be sent via 'setup', 'step', and 'finish' methods.
        Returns a dictionary of dictionaries:
        {key: <some int>: {
            "description": <some string>, "user_details": <a dictionary of twitter user-details results>, "similarities": <a list of user-ids that bind the group>}
        }
        '''
        if notification_hook:
            step_count = 2 + USER_CALL_COUNT + 2
            self.notification_hook = notification_hook
            notification_hook.setup(step_count)
        self.restapi.notifier = notification_hook
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
        #join rest api calling and result processing green threads 
        pool = gevent.pool.Pool(size=20)
        self.following_queue = Queue()
        bucket_gen = pool.spawn(self._generate_follower_buckets, len(user_details))
        following_calls = [pool.spawn(self._following_call, user["id"]) for user in user_details]
        gevent.joinall(following_calls + [bucket_gen])
        groups = dict(self._gen_buckets_to_groups(self.follower_buckets))
        if notification_hook:
            notification_hook.finish()
        return groups

    def _extract_power_user_group(self, user_details):
        '''removes power users from consideration based on the SUPER_USER_FILTER integer'''
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
        '''get followeds for user'''
        following = None
        try:
            resp = self.restapi.following(user_id=user_id)
            following = (user_id, resp["ids"])
        except:
            logger.exception("Call for user_id %i failed", user_id)
        finally:
            self.following_queue.put(following) 

    @notify
    def _generate_follower_buckets(self, bucket_count):
        '''convert list of followers and their followeds to list of followeds and their followers'''
        # queue-querying generator
        def user_foll_gen():
            for _ in xrange(bucket_count):
                queued = self.following_queue.get()
                if queued is not None:
                    yield queued
        follower_buckets = {}
        for user, following in user_foll_gen():
            for followed in following:
                if followed in follower_buckets:
                    follower_buckets[followed].add(user)
                else:
                    follower_buckets[followed] = Set([user])
        # filter out low scoring followeds and convert to list of set 2-tuples
        follower_buckets = list((Set([k]),v) for k,v in follower_buckets.items() if len(v) > FOLLOWED_COUNT_FILTER)
        self.follower_buckets = self._consolidate_follower_buckets(follower_buckets)

    def _consolidate_follower_buckets(self, follower_buckets):
        '''Merge buckets that have enough followers in common, calculated using the INTERSECTION_FILTER multiplier.'''
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
        # filter out all buckets with a low correspondence score
        follower_buckets = [(similarities, user_ids) 
            for similarities, user_ids in merged_buckets 
                if len(similarities) > CORRESPONDENCE_FILTER] 
        return follower_buckets

    @notify
    def _gen_buckets_to_groups(self, follower_buckets):
        '''converts sets of ids to lists of full user details'''
        index = 0
        for similarities, user_ids in follower_buckets:
            user_det = [self.user_det[uid] for uid in user_ids if uid in self.user_det]
            description = ""
            count = 0
            for user in user_det:
                for word in user["description"].split(' '):
                    word = "".join(c for c in word if c not in (',', '.', "'", '"')).title()
                    if len(word) > 4:
                        description += word
                        if count == 0:
                            description += "'s "
                        elif count == 1:
                            description += " "
                        count += 1
                        break
                if count > 2:
                    break
            yield (index, {"description": description, "user_details": user_det, "similarities": list(similarities)})
            index += 1


def test():
    class ProgressNotifier(object):
        def __init__(self):
            self.step_state = 0

        def setup(self, step_count):
            self.step_count = step_count
            print self.step_state

        def step(self):
            step_value = 100.0 / self.step_count
            self.step_state += step_value 
            print self.step_count, int(self.step_state)
            
        def finish(self):
            self.step_state = 100
            print self.step_state

    logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)
    api = TwitterRestAPI(cache=atrest.Cache(atrest.FileBackend('/tmp/atrest_cache'), 3600))
    grouper = Grouper(api)
    nh = ProgressNotifier()
    groups = grouper.generate_groups(user_name='phildlv', notification_hook=nh)
    for desc, followers in groups:
        print desc
        print [user["description"] for user in followers]
        print ''

if __name__ == '__main__':
    test()
