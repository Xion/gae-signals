'''
gae-signals
Signals' library for Google App Engine
@author Karol Kuczmarski "Xion"
'''
from google.appengine.api import memcache
from time import time


class Signal(object):
	''' Represents the signal, identified by a name. '''
	NAMESPACE = 'gae-signals'

	def __init__(self, name):
		self.name = name

	def listen(self, listener):
		''' Adds new listener for the signal.
		@param listener: A callable or fully qualified name of thereof
		'''
		pass

	def send(self, data=None):
		''' Sends the signal to all registered listeners.
		The signal will be intercepted at subsequent request,
		depending on delivery options.
		@param data: Optional data to be included with the signal.
					 If used, it should be a pickleable object.
		'''
		pass


class Lock(object):
	''' Memcached-based lock, providing mutual exclusion. '''
	NAMESPACE = 'gae-signals__locks'

	def __init__(self, key):
		if not key:
			raise ValueError, "Lock needs an unique identifier (key)"
		self.key = key

	def try_acquire(self):
		''' Attempts to acquire the lock. Does not block.
		@return: Whether the lock could be acquired successfully
		'''
		locked = memcache.add(self.key, time(), namespace = self.NAMESPACE)
		return locked

	def release(self):
		''' Releases the lock. Does nothing if the lock has not been acquired. '''
		memcache.delete(self.key, namespace = self.NAMESPACE)


class SignalsMiddleware(object):
	''' WSGI middleware for gae-signals. Ensures that pending singals
	get processed before proceeding with handling the request.
	'''
	def __init__(self, app):
		self.app = app

	def __call__(self, environ, start_response):
		# nothing yet
		return self.app(environ, start_response)