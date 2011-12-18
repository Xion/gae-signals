'''
gae-signals
Signals' library for Google App Engine
'''

__author__ = "Karol Kuczmarski (karol.kuczmarski@gmail.com)"
__copyright__ = "Copyright 2011, Karol Kuczmarski"
__license__ = "MIT"
__version__ = "0.2.1"


from google.appengine.api import memcache
from itertools import izip, repeat, starmap, product
from time import time
import collections


__all__ = ['send', 'deliver', 'SignalsMiddleware', 'SignalMapping']


MESSAGES_NAMESPACE = 'gae-signals__messages'

def send(signal, data=None, reliable=False):
    ''' Sends a signal signal to registered listeners.
    @param signal: Name of signal to send
    @param data: Optional data to be sent with the signal
    @param reliable: Whether the sending should be reliable,
                     i.e. use Compare-And-Set to "synchronize" on list of messages
    '''
    if not signal:
        raise ValueError, "Signal name must not be empty"

    def append_data(_, messages):
        messages = messages or []
        messages.append(data)
        return messages

    return memcache_update(signal, append_data,
                           namespace=MESSAGES_NAMESPACE, reliable=reliable)


def send_multi(signals, reliable=False):
    ''' Sends multiple signals at once, saving on memcache calls.
    @param signals: Iterable of signal names or pairs (signal, data),
                    or a dictionary mapping signal names to data
    @param reliable: Whether the sending should be relialble
    '''
    if not signals: return

    if not isinstance(signals, collections.Mapping):
        signals = dict(((s, None) if isinstance(s, basestring) else s)
                       for s in signals)
    
    def append_data(signal, messages):
        data = signals[signal]
        messages = messages or []
        messages.append(data)
        return messages

    return memcache_update(signals.keys(), append_data,
                           namespace=MESSAGES_NAMESPACE, reliable=reliable)


###############################################################################
# Signal delivery

def deliver(signal_mapping, reliable=False):
    ''' Delivers pending signals using the specified signal mapping.
    @param signal_mapping: A dictionary (or list of pairs) mapping signal names
                           to their listeners
    @return: Number of messages delivered
    '''
    if isinstance(signal_mapping, SignalMapping):
        return signal_mapping.deliver()

    mapping = SignalMapping(reliable=mapping) if reliable else SignalMapping(weak=mapping)
    return mapping.deliver()


class SignalMapping(object):
    ''' Represents a mapping from signal names to its listeners. '''

    def __init__(self, weak=[], reliable=[]):
        ''' Initializes the signal mapping.
        @param weak: Mapping for signals that can be delivered unreliably,
                     e.g. more than once or even not at all
        @param reliable: Mapping for signals that should be delivered reliably,
                         i.e. exactly once
        '''
        self.weak_mapping = self.__preprocess_mapping(weak)
        self.reliable_mapping = self.__preprocess_mapping(reliable)

    def deliver(self, weak=True, reliable=True):
        ''' Delivers pending signals to listeners specified within this signal mapping.
        @return: Number of signals delivered
        '''
        delivered = 0

        if self.reliable_mapping:
            delivered += self.__deliver_reliably(self.reliable.mapping)
        if self.weak_mapping:
            delivered += self.__deliver_weakly(self.weak_mapping)

        return delivered

    def __preprocess_mapping(self, signal_mapping):
        ''' Goes over the specified signal mapping and turns it into
        a dictionary, mapping signal names to lists of listeners that shall handle them.
        @return: A dictionary described above
        '''
        mapping = {}
        for signal_name, listeners in dict(signal_mapping).iteritems():
            if callable(listeners):
                listeners = [listeners]
            elif not isinstance(listeners, collections.Iterable):
                raise ValueError, "Invalid listener(s): %r" % listeners
            mapping[signal_name] = listeners

        return mapping

    def __deliver_reliably(self, signal_mapping_dict):
        ''' Delivers messages reliably, using the specified signal mapping dictionary.
        It uses a memcache lock for every signal in the mapping.
        @return: Number of messages delivered
        '''
        def deliver_signal(signal, listeners):
            with Lock(signal):
                messages = memcache.get(signal, namespace=MESSAGES_NAMESPACE) or []
                listener_args = izip(repeat(signal), messages)
                cross_call(listeners, listener_args)
                memcache.set(signal, [], namespace=MESSAGES_NAMESPACE)
            return len(messages)
        
        return sum(starmap(deliver_signal, signal_mapping_dict.iteritems()))

    def __deliver_weakly(self, signal_mapping_dict):
        ''' Delivers messages weakly, using the specified signal mapping dictionary.
        It uses only two memcache calls for whole mapping but there is no guarantee
        of actual delivery being done exactly once (or even being done at all).
        @return: Number of messages delivered
        '''
        empty_messages = dict((k, []) for k in signal_mapping_dict.keys())

        # get pending messages and immediately replace them with empty lists in memcache
        # (this maximally reduces the time-window where something hazardous can happen)
        messages_dict = memcache.get_multi(signal_mapping_dict.keys(), namespace=MESSAGES_NAMESPACE)
        memcache.set_multi(empty_messages, namespace=MESSAGES_NAMESPACE)

        def deliver_signal(signal, listeners):
            messages = messages_dict.get(signal) or []
            listener_args = izip(repeat(signal), messages)
            cross_call(listeners, listener_args)
            return len(messages)
        
        return sum(starmap(deliver_signal, signal_mapping_dict.iteritems()))


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
# Utilities

def memcache_update(keys, func, time=0, namespace=None, reliable=False):
    ''' Updates a value in memcache by performing a specified function on it.
    @param key: One or more keys of values to be updated
    @param func: Function used to obtain new value. It should accept two 
                 arguments: key and value, and return the new value.
                 If 'reliable' is True, it better be a pure function,
                 for it can be invoked many times
    @param time: Expiration time for updated value
    @param namespace: Memcache namespace where the value resides
    @param reliable: Whether the update should be reliable (and use .cas())
                     or not (and use regulary .set())
    '''
    if isinstance(keys, basestring):
        keys = [keys]

    mc = memcache.Client() if reliable else memcache
    mc_set = getattr(mc, 'cas_multi' if reliable else 'set_multi')

    while keys:
        values = mc.get_multi(keys, namespace = namespace, for_cas=reliable)
        values = dict([(key, func(key, value)) for key, value in values.iteritems()])
        keys = mc_set(values, namespace = namespace)


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


def cross_call(functions, arguments, quelch_exceptions=True):
    ''' Helper function that calls all given functions with all given arguments.
    @param functions: Functions to be called
    @param arguments: Iterable of positional arguments' lists for functions
    @param quelch_exceptions: If True, any exceptions risen from the calls will be ignored
    @return: Iterable with functions' results
    '''
    def invoke(func, args):
        try:
            return func(*args)
        except:
            if not quelch_exceptions:
                raise
    
    for func, args in product(functions, arguments):
        invoke(func, args)
