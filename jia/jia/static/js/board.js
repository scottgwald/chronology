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
  var location = $location.absUrl().split('/');
  var board_id = location[location.length - 1];
  $scope.editorOptions = {
    lineWrapping : true,
    lineNumbers: true,
    mode: 'python',
  };
  $scope.timeseriesOptions = {
    renderer: 'line'
  };
  $scope.timeseriesFeatures = {
    palette: 'spectrum14',
    xAxis: {},
    yAxis: {},
    hover: {},
  };
  $scope.callSource = function(panel) {
    panel.cache.loading = true;
    $http({method: 'POST', url: '/callsource', data: panel.data_source}).
      success(function(data, status, headers, config) {
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
      }).
      error(function(data, status, headers, config) {
        // TODO(marcua): display error.
        console.log(data);
      }).
      finally(function() {
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
    $http({method: 'POST', url: '/board/' + board_id, data: data}).
      success(function(data, status, headers, config) {
        console.log('saved');
      }).
      error(function(data, status, headers, config) {
        console.log('error!');
      });
  };
  $http({method: 'GET', url: '/board/' + board_id}).
    success(function(data, status, headers, config) {
      angular.forEach(data.panels, function(panel) {
        panel.cache = {series: [{name: 'series', data: [{x: 0, y: 0}]}]};
      });
      $scope.boardData = data;
    });
}]);