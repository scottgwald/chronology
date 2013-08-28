// Javascript stores `Date` objects with millisecond accuracy. Convert this to
// the 100ns resolution that Kronos uses.
Date.prototype.toKronosTime = function() {
  return this.getTime() * 10000;
};


// A sorting function for the 'Time' column in table views.
$.tablesorter.addParser({
  id: "jia-time",
  is: function(s, table, cell) {
    return false;
  },
  format: function(s, table, cell, cellIndex) {
    return Date.parse(s).getTime();
  },
  type: "numeric"
});


// `VisModel` contains all the information need to create a visualization.
var VisModel = function(args) {
  this.formula = args.formula;
  this.type = args.type || 'plot';
  this.start = args.start || 'yesterday';
  this.end = args.end || 'now';
  this.properties = args.properties || [];

  this.timeseries = args.timeseries || null;
  this.tabular = this.timeseriesToTabular();
  this.csvURI = this.tabularToCsvUri();
};


// Convert timeseries data to tabular format.
// timeseries = {stream1: [{x: 1, y:1}, ...], ...}
// tabular = [ ['time', 'stream1', 'stream2', ...],
//             [t1, v1, v2, ...], ...]
VisModel.prototype.timeseriesToTabular = function() {
  if (!this.timeseries) {
    return null;
  }

  var headers = _.keys(this.timeseries);

  var tablified_data = {};
  _.each(this.timeseries, function(points, header) {
    _.each(points, function(value) {
      var time = value.x;
      tablified_data[time] = tablified_data[time] || {};
      tablified_data[time][header] = value.y;
    });
  });

  var rows = [['time']];
  rows[0].push.apply(rows[0], headers);
  _.each(tablified_data, function(points, time) {
    var row = [time];
    _.each(headers, function(header) {
      row.push(points[header] || '');
    });
    rows.push(row);
  });

  return rows;
}

// Take data in tabular format (described above) and return a string that
// contains the data in csv format as a data uri so that it can be a link
// target.
VisModel.prototype.tabularToCsvUri = function() {
  if (!this.tabular) {
    return null;
  }

  // TODO(meelap): Escape CSV properly incase it contains commas, etc.
  var csv_rows = _.map(this.tabular, function(row) { return row.join(','); });
  return 'data:text/csv,' + encodeURIComponent(csv_rows.join('\n'));
}

var jia = angular.module('jia', []);

// When the URL changes, run the RouteController to update the models and views.
jia.config(function($routeProvider, $locationProvider) {
  $locationProvider.html5Mode(false);
  $routeProvider.when('/index.html#*state', {
    reloadOnSearch: false,
    controller: RouteController,
  });
});

// Allow data URIs.
jia.config(function($compileProvider) {
  $compileProvider.urlSanitizationWhitelist(/^\s*(https?|data):/);
});

// A service to keep track of the models being visualized.
jia.factory('pageState', function() {
  var models = [];
  return {
    getModels: function() {
      return models;
    },

    addModel: function(model) {
      models.push(model);
    },

    removeModel: function(index) {
      models.splice(index, 1);
    },

    clearModels: function() {
      // Clear the array without breaking anyone else's reference to `models`.
      models.length = 0;
    }
  };
});

// A service to fetch timeseries data from the Jia server.
// Usage:
//  dataFetcher.fetchModel({
//    formula: <string containing stream name>,
//    start: <string parseable by DateJS>,
//    end: <string parseable by DateJS>,
//    type: 'plot'|'table',
//    properties: <list of properties on stream to fetch. empty list means all>
//  })
//  returns a promise object P.
//  Use P.then(function(model) {...}) to process the returned model.
jia.factory('dataFetcher', function($http, $q) {
  var dataFetcherInstance = {};
  dataFetcherInstance.fetchModel = function(args) {
    var q = $q.defer();
    $http({
      method: 'POST',
      url: '/get',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      data: $.param({
        stream_name: args.formula,
        start_time: Date.parse(args.start).toKronosTime(),
        end_time: Date.parse(args.end).toKronosTime(),
        properties: args.properties
      }),
    }).success(function(data, status) {
      if (_.has(data, 'error')) {
        // TODO(meelap): retry, then show user the error
        console.log('Server side error fetching data points:'+data);
      } else {
        q.resolve(new VisModel({
          formula: args.formula,
          type: args.type,
          start: args.start,
          end: args.end,
          properties: args.properties,
          timeseries: data
        }));
      }
    }).error(function(data, status) {
      // TODO(meelap): retry, then show user the error
      console.log('Failed to fetch data points from Jia server.');
    });
    return q.promise;
  };
  return dataFetcherInstance;
});

// Adds autocomplete to an input text element.
// Usage:
//  <input type=text ng-model=model autocomplete=list_of_items>
// where list_of_items is a list of strings defined on the parent $scope.
jia.directive('autocomplete', function() {
  return {
    restrict: 'A',
    require: 'ngModel',
    scope: {
      formula: '=ngModel',
      source: '=autocomplete',
    },
    link: function($scope, elm, attrs, ctrl) {
      var validFormula = function (viewValue) {
        if (_.contains($scope.source, viewValue)) {
          ctrl.$setValidity('autocomplete', true);
          return viewValue;
        } else {
          ctrl.$setValidity('autocomplete', false);
          return undefined;
        }
      };
      ctrl.$parsers.unshift(validFormula);

      var $elm = $(elm);

      // Angular isn't able to capture changes in the input field when the
      // jQuery autocomplete widget is active, so manually update the model.
      elm.bind('keypress', function(event) {
        $scope.$apply(function() {
          $scope.formula = $elm.val();
        });
      });

      $elm.autocomplete({
        source: [],
        close: function() {
          $scope.$apply(function() {
            $scope.formula = $elm.val();
            validFormula($scope.formula);
          });
        }
      });

      $scope.$watch('source', function(source) {
        $elm.autocomplete('option', 'source', source);
      });
    }
  };
});

// Add `date` to an input element to set the ng-valid and ng-invalid CSS classes
// when the input value is or isn't DateJS parseable.
jia.directive('date', function() { return {
    require: 'ngModel',
    restrict: 'A',
    link: function($scope, elm, attrs, ctrl) {
      ctrl.$parsers.unshift(function(viewValue) {
        if (Date.parse(viewValue)) {
          ctrl.$setValidity('date', true);
          return viewValue;
        } else {
          ctrl.$setValidity('date', false);
          return undefined;
        }
      });
    }
  };
});

// Use the `timeseries-table` tag with the tabular attribute set to create a
// table.
// Usage:
//  <timeseries-table x-tabular=my_tabular_data />
// Where my_tabular_data is in the format of VisModel.tabular.
jia.directive('timeseriesTable', function($timeout) {
  return {
    restrict: 'E',
    replace: true,
    scope: {
      tabular: '='
    },
    template:
      '<table class="tablesorter">' +
      '  <thead>' +
      '    <tr>' +
      '      <th class="sorter-jia-time">Time (UTC)</th>' +
      '      <th ng-repeat="header in headers">{{header}}</th>' +
      '    </tr>' +
      '  </thead>' +
      '  <tbody>' +
      '    <tr ng-repeat="row in rows">' +
      '      <td ng-repeat="cell in row track by $index">{{cell}}</td>' +
      '    </tr>' +
      '  </tbody>' +
      '</table>',
    link: function($scope, elm, attrs) {
      $scope.headers = _.rest($scope.tabular[0]);
      var rows = $scope.rows = _.rest($scope.tabular);
      _.each(rows, function(row) {
        row[0] = new Date(1000 * row[0]).toDateString();
      });
      $timeout(function() {
        $(elm).tablesorter({
          sortList: [[0,1]],
          widgets: ['zebra'], 
        });        
      }, 0);
    },
  };
});

// Use the `timeseries-plot` tag with the tabular attribute set to create a
// plot.
// Usage:
//  <timeseries-plot x-timeseries=my_timeseries_data />
// Where my_timeseries_data is in the format of VisModel.timeseries.
jia.directive('timeseriesPlot', function($timeout) {
  return {
    restrict: 'E',
    replace: true,
    scope: {
      timeseries: '=',
    },
    template:
      '<div class="plot-container">' +
      '  <div class="y_axis"></div>' +
      '  <div class="plot"></div>' +
      '  <div class="legend"></div>' +
      '</div>',
    link: function($scope, elm, attrs) {
      $timeout(function() {
        var R = Rickshaw, G = R.Graph, S = G.Behavior.Series;
        var $elm = $(elm);

        $scope.data = new R.Series(
          _.map($scope.timeseries, function(points, stream) {
            return {
              name: stream,
              data: points,
              disabled: stream[0] == '$' 
            };
          }), {scheme: 'munin'});
        R.Series.zeroFill($scope.data);

        $scope.graph = new G({
          element: $elm.find('.plot')[0],
          interpolation: 'linear',
          height: 400,
          series: $scope.data,
          renderer: 'area',
        });

        $scope.legend = new G.Legend({
          graph: $scope.graph,
          element: $elm.find('.legend')[0]
        });

        $scope.toggler = new S.Toggle({
          graph: $scope.graph,
          legend: $scope.legend
        });

        $scope.highlighter = new S.Highlight({
          graph: $scope.graph,
          legend: $scope.legend
        });

        $scope.detail = new G.HoverDetail({graph: $scope.graph});
        $scope.x_axis = new G.Axis.Time({graph: $scope.graph});

        $scope.y_axis = new G.Axis.Y({
          graph: $scope.graph,
          orientation: 'left',
          element: $elm.find('.y_axis')[0],
          tickFormat: R.Fixtures.Number.formatKMBT
        });

        $scope.graph.render();
        $scope.x_axis.render();
        $scope.y_axis.render();
      }, 0);
    }
  };  
});

// A simple checkbox element.
jia.directive('checkbox', function() {
  return {
    restrict: 'E',
    replace: true,
    transclude: true,
    scope: {
      checked: '=model',
      disabled: '='
    },
    template:
      '<label class="checkbox"' +
      '       ng-transclude>' +
      '<input type=checkbox' +
      '       ng-model="checked"' +
      '       ng-disabled="disabled"' +
      '       >' +
      '</label>',
  };
});


// The controller for the 'Create a new visualization' form.
function FormController($scope, $http, pageState, dataFetcher) {
  $scope.init = function() {
    $scope.form = {
      vis_type: 'plot',
      property_type: 'all',
      start_time: '',
      end_time: '',
      formula: '',
    };
    $scope.stream_properties = {};

    // Load the list of streams so that we can autocomplete.
    $http({
      method: 'GET',
      url: '/streams',
    }).success(function(data, status) {
      var s = $scope.stream_properties = {};
      _.each(data['streams'], function(properties, stream_name) {
        var p = s[stream_name] = [];
        _.each(properties, function(property) {
          p.push({
            value: property,
            checked: true
          });
        });
      });
      $scope.streams = _.keys(s);
    }).error(function(data, status) {
      // TODO(meelap): retry, then show user the error
      console.log('Failed to fetch stream names from Jia server.');
    });
  };

  // When the formula changes, update the list of visible properties to match.
  $scope.$watch('form.formula', function(new_formula) {
    if (new_formula) {
      $scope.properties = $scope.stream_properties[new_formula] || [];
    } else {
      $scope.properties = [];
    }
  });

  // Disable property checkboxes when 'all properties' is selected.
  $scope.$watch('form.property_type', function(property_type) {
    $scope.properties_disabled = property_type == 'all';
  });

  // When the form is submitted, fetch the new model and visualize it.
  $scope.addNewVis = function(form) {
    var properties = [];
    if (form.property_type == 'select') {
      _.each($scope.properties, function(property) {
        if (property.checked) {
          properties.push(property.value);
        }
      });
    }

    dataFetcher.fetchModel({
      type: form.vis_type,
      formula: form.formula,
      start: form.start_time,
      end: form.end_time,
      properties: properties
    }).then(pageState.addModel);
  };
}

// This controller keeps the state in the URL and the views on the page in sync.
function RouteController($scope, $location, pageState, dataFetcher) {
  // Compute a string representing the state of the page.
  $scope.hashModels = function() {
    var hashModel = function(model) {
      var p = (model.properties && model.properties.join(',')) || '';
      return [model.type, model.formula, model.start, model.end, p].join(',');
    };
    return (_.map(pageState.getModels(), hashModel)).join('#');
  };

  // Load application state from a string.
  $scope.loadHash = function(hash) {
    pageState.clearModels();
    _.each(hash.split('#'), function(model) {
      var parts = model.split(',');
      if (parts.length >= 4) {
        dataFetcher.fetchModel({
          type: parts[0],
          formula: parts[1],
          start: parts[2],
          end: parts[3],
          properties: (parts[4] && parts.slice(4)) || []
        }).then(pageState.addModel);
      }
    });
  };

  // Load the state from the URL whenever the URL changes.
  $scope.loadHash($location.hash());

  // Update the URL when the page state changes.
  // TODO(meelap): Deep equality checking on models might be expensive. Could
  // switch to only checking the number of models.
  $scope.$watch(pageState.getModels, function() {
    $location.hash($scope.hashModels());
  }, true);
}

function ViewController($scope, pageState) {
  $scope.getModels = pageState.getModels;
  $scope.removeView = function(index) {
    pageState.removeModel(index);
  };
}
