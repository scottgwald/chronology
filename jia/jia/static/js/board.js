var app = angular.module('boardApp', ['ui.codemirror',
                                      'angular-rickshaw'
                                     ]);

app.config(['$interpolateProvider', function($interpolateProvider) {
  // Using {[ ]} to avoid collision with server-side {{ }}.
  $interpolateProvider.startSymbol('{[');
  $interpolateProvider.endSymbol(']}');
}]);

app.controller('boardController',
['$scope', '$http', '$location', '$timeout',
function ($scope, $http, $location, $timeout) {
  // TODO(marcua): Re-add the sweet periodic UI refresh logic I cut
  // out of @usmanm's code in the Angular rewrite.
  var location = $location.absUrl().split('/');
  var board_id = location[location.length - 1];
  $scope.editorOptions = {
    lineWrapping : true,
    lineNumbers: true,
    mode: 'python',
    theme: 'mdn-like',
  };
  $scope.timeseriesOptions = {
    renderer: 'line',
    width: parseInt($('.panel').width() * .73)
  };
  $scope.timeseriesFeatures = {
    palette: 'spectrum14',
    xAxis: {},
    yAxis: {},
    hover: {},
  };
  $scope.callAllSources = function() {
    _.each($scope.boardData.panels, function(panel) {
      $scope.callSource(panel);
    });
  };
  $scope.callSource = function(panel) {
    panel.cache.loading = true;
    // TODO(marcua): do a better job of resizing the plot given the
    // legend size.
    $scope.timeseriesOptions.width = parseInt($('.panel').width() * .73);
    $http.post('/callsource', panel.data_source)
      .success(function(data, status, headers, config) {
        // `data` should contain an `events` property, which is a list
        // of Kronos-like events.  The events should be sorted in
        // ascending time order.  An event has two fields `@time`
        // (Kronos time: 100s of nanoseconds since the epoch), and
        // `@value`, a floating point value.  An optional `@group`
        // attribute will split the event stream into different
        // groups/series.  All points in the same `@group` will be
        // plotted on their own line.
        var series = _.groupBy(data.events, function(event) {
          return event['@group'] || 'series';
        });
        delete $scope.timeseriesFeatures.legend;
        if (_.size(series) > 0) {
          series = _.map(series, function(events, seriesName) {
            return {name: seriesName, data: _.map(events, function(event) {
              return {x: event['@time'] * 1e-7, y: event['@value']};
            })}
          });
          if (_.size(series) > 1) {
            $scope.timeseriesFeatures.legend = {toggle: true, highlight: true};
          }
        } else {
          series = [{name: 'series', data: [{x: 0, y: 0}]}];
        }
        panel.cache.series = series;
      })
      .error(function(data, status, headers, config) {
        // TODO(marcua): display error.
        console.log(data);
      })
      .finally(function() {
        panel.cache.loading = false;
      });
  }
  $scope.saveBoard = function() {
    // Deep copy the board data and remove the cached data.
    var data = JSON.parse(JSON.stringify($scope.boardData, function(key, value) {
      if (key === 'cache') {
        return undefined;
      }
      return value;
    }));
    // TODO(marcua): display something on save success/failure.
    $http.post('/board/' + board_id, data)
      .success(function(data, status, headers, config) {
        console.log('saved');
      })
      .error(function(data, status, headers, config) {
        console.log('error!');
      });
  };
  $scope.initPanel = function(panel) {
    panel.cache = {series: [{name: 'series', data: [{x: 0, y: 0}]}]};
  }
  $scope.addPanel = function() {
    $scope.boardData.panels.unshift({
      title: '',
      data_source: {
        source_type: 'pycode',
        refresh_seconds: null,
        code: ''
      },
      display: {
        display_type: 'timeseries'
      }
    });
    $scope.initPanel($scope.boardData.panels[0]);
  };
  $http.get('/board/' + board_id)
    .success(function(data, status, headers, config) {
      angular.forEach(data.panels, function(panel) {
        $scope.initPanel(panel);
      });
      $scope.boardData = data;
    });
}]);
