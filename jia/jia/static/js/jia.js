Date.prototype.toKronosTime = function() {
  return this.getTime() * 10000;
};

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

var jia = angular.module('jia', []);

jia.directive('autocomplete', function() {
  return {
    restrict: 'A',
    require: 'ngModel',
    scope: {
      formula: '=ngModel',
      source: '=autocomplete',
    },
    link: function($scope, elm, attrs, ctrl) {
      function validFormula(viewValue) {
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

jia.directive('date', function() {
  return {
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
          }
          ), {scheme: 'munin'});
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

jia.config(['$compileProvider', function($compileProvider) {
  $compileProvider.urlSanitizationWhitelist(/^\s*(https?|data):/);
}]);

function JiaController($scope, $http) {
  $scope.init = function() {
    $scope.form = {
      vis_type: 'plot',
      property_type: 'all',
      start_time: '',
      end_time: '',
      formula: '',
    };
    $scope.stream_properties = {};

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
    $scope.visualizations = [];
  };

  $scope.$watch('form.formula', function(new_formula) {
    if (new_formula) {
      $scope.properties = $scope.stream_properties[new_formula] || [];
    } else {
      $scope.properties = [];
    }
  });

  $scope.$watch('form.property_type', function(property_type) {
    $scope.properties_disabled = property_type == 'all';
  });

  $scope.addNewVis = function(form) {
    var properties = [];
    if (form.property_type == 'select') {
      _.each($scope.properties, function(property) {
        if (property.checked) {
          properties.push(property.value);
        }
      });
    }

    $http({
      method: 'POST',
      url: '/get',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      data: $.param({
        stream_name: form.formula,
        start_time: Date.parse(form.start_time).toKronosTime(),
        end_time: Date.parse(form.end_time).toKronosTime(),
        properties: properties
      }),
    }).success(function(data, status) {
      if (_.has(data, 'error')) {
        // TODO(meelap): retry, then show user the error
        console.log('Server side error fetching data points:'+data);
      } else {
        var newvis = {
          formula: form.formula,
          vis_type: form.vis_type,
          timeseries: data,
          tabular: timeseriesToTabular(data),
        };
        $scope.visualizations.push(newvis);
      }
    }).error(function(data, status) {
      // TODO(meelap): retry, then show user the error
      console.log('Failed to fetch data points from Jia server.');
    });
  };

  $scope.removeVis = function(index) {
    $scope.visualizations.splice(index, 1);
  };
}

function VisController($scope) {
  $scope.csvUri = tabularToCSV($scope.vis.tabular);
}

function tabularToCSV(tabular) {
  // TODO(meelap): Escape CSV properly incase it contains commas, etc.
  var csv_rows = _.map(tabular, function(row) { return row.join(','); });
  return 'data:text/csv,' + encodeURIComponent(csv_rows.join('\n'));
}

function timeseriesToTabular(timeseries) {
  var headers = _.keys(timeseries);

  var tablified_data = {};
  _.each(timeseries, function(points, header) {
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
