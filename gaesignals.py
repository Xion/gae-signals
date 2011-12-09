'''
gae-signals
Signals' library for Google App Engine
@author Karol Kuczmarski "Xion"
'''
from google.appengine.api import memcache
from time import time


class Signal(object):
	''' Represents the signal, identified by a name. '''
	LISTENERS_NAMESPACE = 'gae-signals__listeners'
	MESSAGES_NAMESPACE = 'gae-signals__messages'

	def __init__(self, name):
		if not name:
			raise ValueError, "Signal must have a name"
		self.name = name

	def listen(self, listener):
		''' Adds new listener for the signal.
		@param listener: A callable or fully qualified name of thereof
		'''
		with Lock(self.name):
			listeners = memcache.get(self.name, namespace = LISTENERS_NAMESPACE)
			listeners = set(listeners or [])
			listeners.add(self.__get_listener_name(listener))
			memcache.set(self.name, namespace = LISTENERS_NAMESPACE)

	def try_send(self, data=None):
		''' Attempts to send the signal to registered listeners.
		This might fail if it's impossible to immediately acquire the lock
		on signal's message queue.
		@return: Whether sending succeeded
		'''
		lock = Lock(self.name)

		try:
			if not lock.try_acquire():
				return False
			self.__send(data)
		finally:
			lock.release()

	def send(self, data=None):
		''' Sends the signal to all registered listeners.
		The signal will be intercepted at subsequent request,
		depending on delivery options.
		@param data: Optional data to be included with the signal.
					 If used, it should be a pickleable object.
		'''
		with Lock(self.name):
			self.__send(data)

	def __get_listener_name(self, listener):
		''' Gets the fully qualified name of listener. '''
		if isinstance(listener, basestring):
			return listener
		return '%s.%s' % (listener.__module__, listener.__name__)

	def __send(self, data=None):
		''' Sends the signal to registered listeners, queueing
		it for delivery in subsequent requests.
		@warning: This should be invoked within a lock
		'''
		messages = memcache.get(self.name, namespace = MESSAGES_NAMESPACE) or []
		messages.append(data)
		memcache.set(self.name, messages, namespace = MESSAGES_NAMESPACE)


class Lock(object):
	''' Memcached-based lock, providing mutual exclusion. '''
	NAMESPACE = 'gae-signals__locks'

	def __init__(self, key):
		if not key:
			raise ValueError, "Lock needs an unique identifier (key)"
		self.key = key

	def __enter__(self):
		self.acquire()

	def __exit__(self, _, _, _):
		self.release()

	def try_acquire(self):
		''' Attempts to acquire the lock. Does not block.
		@return: Whether the lock could be acquired successfully
		'''
		locked = memcache.add(self.key, time(), namespace = self.NAMESPACE)
		return locked

	def acquire(self):
		''' Acquires the lock. May block indefinetely. '''
		while not self.try_acquire():
			pass

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