from google.appengine.api import memcache
from datetime import datetime


def foo(data):
	memcache.set('foo', data)