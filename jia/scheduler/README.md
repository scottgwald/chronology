# Jia Task Scheduler

## Running
Be sure to modify the relevant settings in `settings.cfg`

```
# Precompute settings
PRECOMPUTE = True
SCHEDULER_HOST = '127.0.0.1' # It is not recommended to expose the scheduler outside the LAN
SCHEDULER_PORT = 8157
SCHEDULER_DATABASE_URI = 'sqlite:///%s/scheduler.db' % APPROOT
```

Always use the following command to start the scheduler. Never serve the
scheduler app from another WSGI server (see the top of runscheduler.py
for details).
```
$ python runscheduler.py
```

## Scheduling
```
from scheduler.api import schedule, cancel

# Prints jia every 5 seconds
result = schedule("print 'jia'", 5)

if result['status'] != 'success':
  print result['reason']
  exit()

# Unschedule the task we just made
result = cancel(result['id'])

if result['status'] != 'success':
  print result['reason']
```
