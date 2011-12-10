'''
App Engine config for gae-signals test application
'''
# sys.path trick to expose the gaesignals module from parent directory
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


from gaesignals import SignalsMiddleware
import sighandlers


def webapp_add_wsgi_middleware(app):
	app = SignalsMiddleware(app, [
		('foo', sighandlers.foo),
	])
	return app