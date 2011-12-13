# gae-signals

Signals library for Google App Engine.

## What is this?

_Signals_ are used in complex applications to decouple event dispatch from event handling.
A typical use case is having one module send a signal (corresponding to some
application-specific event) and have some other module receive it at some later time,
and handle appropriately.

In web applications signals are especially useful. They allow foreground requests
to generate events which defer expensive computation for later time.
Background processes can then have such signals delivered and perform those time-consuming
tasks without increasing visible latency.

_gae-signals_ library provides simple, memcache-based implementation of signals for Google App Engine.
They are quite similar to [Django signals](https://docs.djangoproject.com/en/dev/topics/signals/)
or [Flask signals](http://flask.pocoo.org/docs/signals/), although somewhat simplier.

## Usage

Sending a signal is as easy as invoking <code>send</code> method from the <code>Signal</code> class:

```python
from gaesignals import Signal
Signal('my_signal').send()

# optional data to be passed along
from datetime import datetime
Signal('my_signal').send(datetime.now())
```
Delivery can be performed at any time using the <code>deliver</code> function. It takes a mapping,
associating signal names to their handlers - similarly to _webapp_'s <code>WSGIApplication</code>
routing of request handlers:

```python
from gaesignals import deliver
import logging

def my_signal_handler(data = None):
    logging.info("Received my_signal with data: %r", data)

deliver([
        ('my_signal', my_signal_handler),
        ])
```
Alternatively, you can use the WSGI middleware, which delivers specified signals at the start of request:

```python
# appengine_config.py

from gaesignals import SignalsMiddleware
from myapp.signals import my_signal_handler


def webapp_add_wsgi_middleware(app):
    app = SignalsMiddleware(app, [
        ('my_signal', my_signal_handler)
    ])
    return app
```
In practice, you would want to deliver different signals at different times. For example, signals that
are specific to current user should be dispatched when the user is already authenticated - e.g. in
<code>get</code>/<code>post</code> method of <code>RequestHandler</code> if using the _webapp_ framework.

## Performance considerations

By default, _gae-signals_ minimalizes the overhead of signal delivery by using only two memcache operations
for single call to <code>deliver</code>. That's why it is beneficial to aggregate as many signals as possible
in single mapping.

### Choosing delivery mode

However, this efficient delivery method may rarely result in some signals being lost or delivered more than once.
This can happen especially in times of very intensive usage. If this poses a problem to your application, you can
choose to deliver some signals reliably - at the expense of speed - by passing the <code>reliable</code> parameter
to <code>deliver</code> function:

```python
deliver([
        ('important_signal', important_signal_handler)
        ], reliable=True)
```
This increased the overhead to two memcache calls **per signal**, along with at least two more calls due to using
a memcache-based synchronization lock. Therefore using the default ("weak") delivery mode is recommended, and default.

### Minimalizing contention

For best results, you should use finely-grained signals whose scope is as small as possible. For example,
having a global <code>'user\_signed\_up'</code> may prove troublesome if it's triggered many times per second.
On the other hand, something like <code>user\_54274\_password\_change</code> - that is, signal specific to
a single user - should be absolutely fine in all reasonable cases.

## Notes

_gae-signals_ is licensed under MIT license.