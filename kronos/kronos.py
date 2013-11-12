#!/usr/bin/python

import gevent.monkey; gevent.monkey.patch_all()
import gevent.pywsgi
import geventhttpclient.httplib; geventhttpclient.httplib.patch()

import imp
import multiprocessing
import subprocess
import werkzeug.serving
import os

from argparse import ArgumentParser

from kronos.conf import settings
from kronos.server import wsgi_application

if __name__ == '__main__':
  parser = ArgumentParser(description='Kronos HTTP server.')
  parser.add_argument('--debug', action='store_true', help='debug mode?')
  parser.add_argument('--bind', action='store', default='0.0.0.0:8150',
                      help='hostname:port or unix:/path to listen for '
                      'incoming requests')
  parser.add_argument('--num-proc', action='store',
                      default=multiprocessing.cpu_count() * 2 + 1,
                      help='number of processes to run')
  parser.add_argument('--collector-mode', action='store_true',
                      help='only open the put endpoint?')
  parser.add_argument('--config', action='store',
                      help='path of config file to use')
  parser.add_argument('--print-pid', action='store_true', help='print PID?')
  parser.add_argument('--behind-nginx', action='store_true',
                      help='running behind Nginx?')
  args = parser.parse_args()

  # If a config file path is given, import that as the `settings` module.
  if args.config:
    imp.load_source('kronos.conf.settings', args.config)

  # Override the `debug` and `collector_mode` attributes of the settings
  # module and `debug` for `args`.
  setattr(settings, 'debug',
          getattr(args, 'debug') | getattr(settings, 'debug', False))
  setattr(settings, 'collector_mode',
          getattr(args, 'collector_mode') |
          getattr(settings, 'collector_mode', False))
  setattr(args, 'debug', settings.debug)
  
  if args.debug:
    (host, port) = args.bind.split(':')
    werkzeug.serving.run_with_reloader(
        lambda: gevent.pywsgi.WSGIServer((host, int(port)),
                                         wsgi_application).serve_forever())
  else:
    # Try creating log directory, if missing.
    log_dir = settings.node['log_directory'].rstrip('/')
    if not os.path.exists(log_dir):
      os.makedirs(log_dir)

    if args.behind_nginx:
      port = args.bind
      args.bind = 'unix:/tmp/kronos.%s.sock' % port
      # TODO(usmanm): Use some helper template rendering function.
      with open('kronos/conf/kronos.nginx.template') as nginx_template:
        nginx_conf = nginx_template.read()
        nginx_conf = nginx_conf.replace('{{ socket_path }}', args.bind)
        nginx_conf = nginx_conf.replace('{{ port }}', port)
        with open('kronos.nginx.conf', 'w') as conf:
          conf.write(nginx_conf)
        nginx_conf_path = '/etc/nginx/sites-enabled/kronos.%s.conf' % port
        if os.path.lexists(nginx_conf_path):
          os.remove(nginx_conf_path)
        os.symlink(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                'kronos.nginx.conf')),
                   nginx_conf_path)

    argv = ['gunicorn', 'kronos.server:wsgi_application',
            '--worker-class', 'gevent',
            '--log-level', 'info',
            '--name', 'kronosd',
            '--access-logfile', '{0}/{1}'.format(log_dir, 'access.log'),
            '--error-logfile', '{0}/{1}'.format(log_dir, 'error.log'),
            '--workers', str(args.num_proc),
            '--bind', args.bind]
    proc = subprocess.Popen(argv)
    if args.print_pid:
      print proc.pid

    if args.behind_nginx:
      # Reload nginx.
      subprocess.call(['service', 'nginx', 'reload'])
