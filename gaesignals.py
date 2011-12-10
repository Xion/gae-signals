'''
gae-signals
Signals' library for Google App Engine
@author Karol Kuczmarski "Xion"
'''
from google.appengine.api import memcache
from time import time
import collections
import itertools


class Signal(object):
	''' Represents the signal, identified by a name. '''
	MESSAGES_NAMESPACE = 'gae-signals__messages'

	def __init__(self, name):
		if not name:
			raise ValueError, "Signal must have a name"
		self.name = name

	def try_send(self, data=None):
		''' Attempts to send the signal to registered listeners.
		This might fail if it's impossible to immediately acquire the lock
		on signal's message queue.
		@return: Whether sending succeeded
		'''
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

	def __send(self, data=None):
		''' Sends the signal to registered listeners, queueing
		it for delivery in subsequent requests.
		@warning: This should be invoked within a lock
		'''
		messages = memcache.get(self.name, namespace = MESSAGES_NAMESPACE) or []
		messages.append(data)
		memcache.set(self.name, messages, namespace = MESSAGES_NAMESPACE)


class SignalsMiddleware(object):
	''' WSGI middleware for gae-signals. Ensures that pending singals
	get processed before proceeding with handling the request.
	'''
	def __init__(self, app, signal_mapping=[]):
		self.app = app
		self.__setup_listeners(signal_mapping)

	def __call__(self, environ, start_response):
		self.__deliver_messages()
		return self.app(environ, start_response)

	def __setup_listeners(self, signal_mapping):
		is_valid_listener = lambda l: isinstance(l, basestring) or callable(l)

		self.mapping = {}
		for signal_name, listeners in dict(signal_mapping).iteritems():
			if is_valid_listener(listeners):
				listeners = [listeners]
			elif not isinstance(listeners, collections.Iterable):
				raise ValueError, "Invalid listener(s): %r" % listeners
			self.mapping[signal_name] = listeners

	def __deliver_messages():
		for signal_name, listeners in self.mapping.iteritems():
			signal = Signal(signal_name)
			with Lock(signal.name):
				messages = memcache.get(signal.name, namespace = Signal.MESSAGES_NAMESPACE)
				for msg, listener in itertools.izip(messages, listeners):
					if msg is None:	listener()
					else:			listener(msg)
				memcache.set(signal.name, [], namespace = Signal.MESSAGES_NAMESPACE)


###############################################################################
# Utilities

class Lock(object):
	''' Memcached-based lock, providing mutual exclusion. '''
	NAMESPACE = 'gae-signals__locks'

	def __init__(self, key):
		if not key:
			raise ValueError, "Lock needs an unique identifier (key)"
		self.key = key

	def __enter__(self):
		self.acquire()

	def __exit__(self, exit_type, value, traceback):
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