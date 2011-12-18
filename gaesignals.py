'''
gae-signals
Signals' library for Google App Engine
'''

__author__ = "Karol Kuczmarski (karol.kuczmarski@gmail.com)"
__copyright__ = "Copyright 2011, Karol Kuczmarski"
__license__ = "MIT"
__version__ = "0.1.1"


from google.appengine.api import memcache
from itertools import starmap, product
from time import time
import collections


__all__ = ['Signal', 'SignalMapping', 'deliver', 'SignalsMiddleware']


class Signal(object):
    ''' Represents the signal, identified by a name. '''
    MESSAGES_NAMESPACE = 'gae-signals__messages'

    def __init__(self, name):
        if not name:
            raise ValueError, "Signal must have a name"
        self.name = name

    def send(self, data=None, reliable=False):
        ''' Sends the signal to registered listeners, queueing it
        for deilivery in subsequent requests.
        @param data: Optional data to be sent with the signal
        @param reliable: Whether the sending should be reliable,
                         i.e. use Compare-And-Set to "synchronize" on list of messages
        '''
        mc = memcache.Client() if reliable else memcache
        mc_get = getattr(mc, 'gets' if reliable else 'get')
        mc_set = getattr(mc, 'cas' if reliable else 'set')

        while True:
            messages = mc_get(self.name, namespace = self.MESSAGES_NAMESPACE)
            messages = messages or []
            messages.append(data)
            if mc_set(self.name, messages, namespace = self.MESSAGES_NAMESPACE):
                return True



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
        def deliver_signal(signal_name, listeners):
            with Lock(signal_name):
                messages = memcache.get(signal_name, namespace = Signal.MESSAGES_NAMESPACE) or []
                cross_call(listeners, messages)
                memcache.set(signal_name, [], namespace = Signal.MESSAGES_NAMESPACE)
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
        messages_dict = memcache.get_multi(signal_mapping_dict.keys(), namespace = Signal.MESSAGES_NAMESPACE)
        memcache.set_multi(empty_messages, namespace = Signal.MESSAGES_NAMESPACE)

        def deliver_signal(signal_name, listeners):
            messages = messages_dict.get(signal_name) or []
            cross_call(listeners, messages)
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


def cross_call(functions, arguments, omit_none=True, quelch_exceptions=True):
    ''' Helper function that calls all given functions with all given arguments.
    @param omit_none: If True, arguments that are None will not be passed to functions at all
    @param quelch_exceptions: If True, any exceptions risen from the calls will be ignored
    @return: Iterable with functions' results
    '''
    def invoke(func, arg):
        no_arg = arg is None and omit_none
        try:
            return func() if no_arg else func(arg)
        except:
            if not quelch_exceptions:
                raise
    
    for func, arg in product(functions, arguments):
        invoke(func, arg)
