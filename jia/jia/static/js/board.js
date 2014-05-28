var app = angular.module('boardApp', ['ui.codemirror',
                                      'angular-rickshaw',
                                      'ngTable'
                                     ]);

app.config(['$interpolateProvider', function($interpolateProvider) {
  // Using {[ ]} to avoid collision with server-side {{ }}.
  $interpolateProvider.startSymbol('{[');
  $interpolateProvider.endSymbol(']}');
}]);

app.controller('boardController',
['$scope', '$http', '$location', '$timeout', '$filter', 'ngTableParams',
function ($scope, $http, $location, $timeout, $filter, ngTableParams) {
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
        if (panel.display.display_type == 'timeseries') {
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
        }
        else if (panel.display.display_type == 'table') {
          if (_.size(series) > 0) {
            series = _.map(series, function(events, seriesName) {
              column_names = Object.keys(events[0]);
              cols = [];
              for (name in column_names) {
                if (column_names[name] == '@time') {
                  cols.push({field: 'Time'})
                }
                else {
                  cols.push({field: column_names[name]})
                }
              }
              return {name: seriesName, cols: cols, data: _.map(events, function(event) {
                data = {};
                data['Time'] = Date(event['@time'] * 1e-7);
                Object.keys(event).forEach(function (key) {
                  if (key != '@time') {
                    data[key] = event[key];
                  }
                });
                return data;
              })}
            });
            if (_.size(series) > 1) {
              $scope.timeseriesFeatures.legend = {toggle: true, highlight: true};
            }
          } else {
            console.log('blanks');
            series = [];
          }
          panel.display.table_params = new ngTableParams({
            page: 1,            // show first page
            count: 10,          // count per page
          }, {
            total: series[0].data.length, // length of data
            counts: [], // disable the page size toggler
            getData: function($defer, params) {
              // use built-in angular filter
              var orderedData = params.sorting() ?
                                $filter('orderBy')(series[0].data, params.orderBy()) :
                                series[0].data;
              $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
            }
          });
        }
        else {
          throw "Invalid display type";
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
