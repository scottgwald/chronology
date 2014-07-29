#!/usr/bin/env python

"""Starting point for the Jia Scheduler

Never replace this runscheduler.py script with a WSGI server that serves
from multiple threads/processes. Because each Flask app would receive its own
instance of Scheduler (with its own task queue), this application is not
parallelizable. Running multiple Flask instances will result in creating
duplicate running tasks, especially after restarting the server.
"""

import gipc
import gevent
import gevent.monkey; gevent.monkey.patch_all()
import gevent.pywsgi
import subprocess
import werkzeug.serving

from gevent import Greenlet
from multiprocessing import Process
from scheduler import app
from scheduler.scheduler import Scheduler

if __name__ == '__main__':
    app.scheduler = Scheduler()
    werkzeug.serving.run_with_reloader(
      lambda: gevent.pywsgi.WSGIServer((app.config['SCHEDULER_HOST'],
                                        app.config['PORT']),
                                       app).serve_forever())

