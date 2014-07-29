Jia
===

## Task Scheduler
The Jia task scheduler can be used to enable the precomputation of long-running
queries so the results are always ready when you visit the dashboard.

### Running
Be sure to modify the relevant settings in `settings.cfg`

```
# Precompute settings
PRECOMPUTE = True
CACHE_KRONOS_URL = 'http://localhost:8150' # The cache is written here
                                           # Can be the same as KRONOS_URL
CACHE_KRONOS_NAMESPACE = 'locu_computed' # The namespace for cache streams
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

### Scheduling
In addition to precompute tasks, you can send the scheduler any Python code
you want executed on a regular basis. The scheduler client API provides access
to `schedule` and `cancel` methods.
```
from scheduler.client import schedule, cancel

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


## Creating/Installing Visualizations

Visualizations are stored in `/static/visualizations`. Each Jia visualization is made up of an Angular module and a template. The name of the module must be prefixed with `jia` (for example: `jia.example`). In order to install a visualization, add it as a dependency to the `boardApp` in `board.js`. Also add any necessary JavaScript or CSS dependencies to `board.html`.

The visualization module must define a factory that matches its title (sans the `jia` prefix). This factory should return a `function` that can be instantiated to represent each instance of the visualization in the UI, as well as a `meta` object that contains some parameters about the type of visualization. The factory should return both.

```
return {
  meta: { title: 'example', ... },
  visualization: function () { ... }
}
```

A full example of the simplest possible visualization:

`example.html`
```html
<div>{[ module.data ]}</div>
```

`example.js`
```javascript
var module = angular.module('jia.example', []);

module.factory('example', function () {

  var meta = {
    title: 'example',
    readableTitle: 'Example',
    template: 'example.html',
  };

  var visualization = function () {
    this.meta = meta;
    this.data = [];
    
    this.setData = function (data, msg) {
      this.data = data;
    }
  }

  return {
    meta: meta,
    visualization: visualization
  }
  
});
```

### Meta Parameters
#### `title`
The title of the visualization. Should match the filename and the angular name after the `jia` prefix.

#### `readableTitle`
A title for the visualization that will be shown to the user in the UI.

#### `template`
The relative path to the visualization template. This template may contain angular directives. It has access to a `module` variable, which is an instantiation of the visualization returned by this factory.

### Future Meta Parameters
#### `css` and `js`
Eventually, css and js dependencies could be specified in lists lazy loaded, so editing board.html will not be necessary to install a visualization.

#### `requiredFields` and `optionalFields`
When the UI query builder is a thing, it might be helpful for it to have information on what visualizations can display what kinds of data.
