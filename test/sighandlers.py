from google.appengine.api import memcache
from datetime import datetime
import logging


def foo(signal, data):
	logging.info("Signal '%s' received with data: %s", signal, data)
	memcache.set('foo', data)