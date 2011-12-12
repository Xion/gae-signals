from google.appengine.api import memcache
from datetime import datetime
import logging


def foo(data):
	logging.info("Signal 'foo' received with data: %s", data)
	memcache.set('foo', data)