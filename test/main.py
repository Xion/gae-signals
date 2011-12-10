'''
gae-signals test application
'''
from gaesignals import Signal
import webapp2
import jinja2
import os


jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.dirname(__file__)))

def render_template(template_file, **params):
	template = jinja_env.get_template(template_file)
	return template.render(params)


class MainPage(webapp2.RequestHandler):
	
	def get(self):
		self.response.out.write(render_template('main.html'))

	def post(self):
		data = self.request.POST.get('data')
		Signal('foo').send(data)
		self.response.out.write(render_template('main.html', data=data))


app = webapp2.WSGIApplication([('/', MainPage)], debug=True)