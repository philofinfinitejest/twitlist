import os
import types
import time
import hashlib
import requests
import cPickle as pickle

'''
Cached objects must be picklable. It is the responsibility of the client to ensure this.
'''

class Cache:
    def __init__(self, backend, timeout_seconds):
        self.timeout_seconds = timeout_seconds
        self.backend = backend

    def store(self, keys, value):
        key = self._make_key(keys)
        data = _Data(time.time() + self.timeout_seconds, value)
        self.backend.put(key, data)

    def fetch(self, keys):
        key = self._make_key(keys)
        data = self.backend.get(key)
        if data is not None and data.timeout < time.time():
            data = None
            self.backend.delete(key)
        return data.data if data is not None else None

    def delete(self, keys):
        key = self._make_key(keys)
        self.backend.delete(key)

    def _make_key(self, keys):
        make_key = hashlib.sha1()
        for key in keys:
            if key is not None:
                make_key.update(key)
        return make_key.hexdigest()


class _Data(object):
    '''Cache DTO'''
    def __init__(self, timeout, data):
        self.timeout = timeout
        self.data = data


class FileBackend(object):
    def __init__(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.directory = directory
    
    def put(self, key, data):
        with open(os.path.join(self.directory, key), 'w') as cache:
            pickle.dump(data, cache)
    
    def get(self, key):
        try:
            with open(os.path.join(self.directory, key), 'r') as cache:
                return pickle.load(cache)
        except IOError:
            return None
    
    def delete(self, key):
        path = os.path.join(self.directory, key)
        try:
            os.remove(path)
        except OSError:
            pass


class BeakerBackend(object):
    def __init__(self, session):
        self.session = session
    
    def put(self, key, data):
        self.session[key] = data
        self.session.save()
    
    def get(self, key):
        data = self.session.get(key)
        return data
    
    def delete(self, key):
        try:
            del self.session[key]
        except KeyError:
            pass
