'''
gae-signals
Signals' library for Google App Engine
@author Karol Kuczmarski "Xion"
'''
from google.appengine.api import memcache
from time import time
import collections
import itertools


__all__ = ['Signal', 'SignalMapping', 'deliver', 'SignalsMiddleware']


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
		messages = memcache.get(self.name, namespace = self.MESSAGES_NAMESPACE) or []
		messages.append(data)
		memcache.set(self.name, messages, namespace = self.MESSAGES_NAMESPACE)


###############################################################################
# Signal delivery

class SignalMapping(object):
	''' Represents a mapping from signal names to its listeners. '''

	def __init__(self, weak=[], reliable=[]):
		''' Initializes the signal mapping.
		@param weak: Mapping for signals that can be delivered unreliably,
					 e.g. more than once or even not at all
		@param reliable: Mapping for signals that should be delivered reliably,
						 i.e. exactly once
		'''
		self.weak_mapping = __preprocess_signal_mapping(weak)
		self.reliable_mapping = __preprocess_signal_mapping(reliable)

	def deliver(self, weak=True, reliable=True):
		''' Delivers pending signals to listeners specified within this signal mapping.
		@return: Number of signals delivered
		'''
		delivered = 0

		if self.reliable_mapping:
			delivered += __deliver_messages_reliably(self.reliable.mapping)
		if self.weak_mapping:
			delivered += __deliver_messages_weakly(self.weak_mapping)

		return delivered
	

def deliver(signal_mapping, reliable=False):
	''' Delivers pending signals using the specified signal mapping.
	@param signal_mapping: A dictionary (or list of pairs) mapping signal names
						   to their listeners
	@return: Number of messages delivered
	'''
	if isinstance(signal_mapping, SignalMapping):
		return signal_mapping.deliver()

	mapping = __preprocess_signal_mapping(signal_mapping)
	deliver_func = __deliver_messages_reliably if reliable else __deliver_messages_weakly
	return deliver_func(mapping)


class SignalsMiddleware(object):
	''' WSGI middleware for gae-signals. Ensures that pending singals
	get processed before proceeding with handling the request.
	'''
	def __init__(self, app, signal_mapping=[]):
		self.app = app
		if not isinstance(signal_mapping, SignalMapping):
			signal_mapping = SignalMapping(signal_mapping)
		self.mapping = signal_mapping

	def __call__(self, environ, start_response):
		self.mapping.deliver()
		return self.app(environ, start_response)


###############################################################################
# Common functions

def __preprocess_signal_mapping(signal_mapping):
	''' Goes over the specified signal mapping and turns it into
	a dictionary, mapping signal names to lists of listeners that shall handle them.
	@return: A dictionary described above
	'''
	is_valid_listener = lambda l: isinstance(l, basestring) or callable(l)

	mapping = {}
	for signal_name, listeners in dict(signal_mapping).iteritems():
		if is_valid_listener(listeners):
			listeners = [listeners]
		elif not isinstance(listeners, collections.Iterable):
			raise ValueError, "Invalid listener(s): %r" % listeners
		mapping[signal_name] = listeners

	return mapping


def __deliver_messages_reliably(signal_mapping_dict):
	''' Delivers messages reliably, using the specified signal mapping dictionary.
	It uses a memcache lock for every signal in the mapping.
	@return: Number of messages delivered
	'''
	delivered = 0

	for signal_name, listeners in signal_mapping_dict.iteritems():
		signal = Signal(signal_name)
		with Lock(signal.name):
			messages = memcache.get(signal.name, namespace = Signal.MESSAGES_NAMESPACE) or []
			__cross_call(listeners, messages)
			memcache.set(signal.name, [], namespace = Signal.MESSAGES_NAMESPACE)
			delivered += len(msg)

	return delivered


def __deliver_messages_weakly(signal_mapping_dict):
	''' Delivers messages weakly, using the specified signal mapping dictionary.
	It uses only two memcache calls for whole mapping but there is no guarantee
	of actual delivery being done exactly once (or even being done at all).
	@return: Number of messages delivered
	'''
	messages_dict = memcache.get_multi(signal_mapping_dict.keys(), namespace = Signal.MESSAGES_NAMESPACE)
	for signal_name, listeners in signal_mapping_dict.iteritems():
		__cross_call(listeners, messages_dict[signal_name])

	empty_messages = dict((k, []) for k in messages_dict.keys())
	memcache.set_multi(empty_messages)

	return reduce(lambda sum, msgs: sum + len(msgs), messages_dict.values())
			

def __cross_call(functions, arguments, omit_none=True):
	''' Helper function that calls all given functions with all given arguments.
	@param omit_none: If True, arguments that are None will not be passed to functions at all
	'''
	for func, arg in itertools.izip(functions, arguments):
		if arg is None:	func()
		else:			func(arg)


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