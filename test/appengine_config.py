'''
App Engine config for gae-signals test application
'''
from gaesignals import SignalsMiddleware
import sighandlers


def webapp_add_wsgi_middleware(app):
	app = SignalsMiddleware(app, [
		('foo', sighandlers.foo),
	])
	return app