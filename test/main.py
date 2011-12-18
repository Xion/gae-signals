'''
gae-signals test application
'''
from gaesignals import send
from google.appengine.api import memcache
import webapp2
import jinja2
import os


jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.dirname(__file__)))

def render_template(template_file, **params):
	template = jinja_env.get_template(template_file)
	return template.render(params)


class MainPage(webapp2.RequestHandler):
	
	def get(self):
		last_foo_data = memcache.get('foo')
		memcache.delete('foo')
		self.response.out.write(render_template('main.html', last=last_foo_data))

	def post(self):
		data = self.request.POST.get('data')
		send('foo', data)
		self.redirect(self.request.path)


app = webapp2.WSGIApplication([('/', MainPage)], debug=True)