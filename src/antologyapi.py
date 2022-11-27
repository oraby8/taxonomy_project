"""anthology api module."""
from gevent import monkey

monkey.patch_all()
from versions.v3.antologyapi import app as api_v3
from flask import Flask
from gevent.pywsgi import WSGIServer

import os

FileDir = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__)

app.register_blueprint(api_v3, url_prefix="/v3")

context = ('Root.crt', 'cert.key')
website_name = 'ontology.linkdev.com:5001'

app.config['SERVER_NAME'] = website_name

if __name__ == '__main__':
    # initialize the log handler
    http_server = WSGIServer(('0.0.0.0', 5001), app, keyfile='cert.key', certfile='Root.crt')
    http_server.serve_forever()

#    app.run(debug=False,ssl_context=context)
