var app = angular.module('boardApp', ['ui.codemirror',
                                      'angular-rickshaw',
                                      'ngTable'
                                     ])

app.config(['$interpolateProvider', function($interpolateProvider) {
  // Using {[ ]} to avoid collision with server-side {{ }}.
  $interpolateProvider.startSymbol('{[');
  $interpolateProvider.endSymbol(']}');
}]);

app.config(['$compileProvider', function($compileProvider) {
  $compileProvider.aHrefSanitizationWhitelist(/^\s*(https?|ftp|mailto|data):/);
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
        panel.cache.data = data;

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
          } else {
            series = [];
          }

          if (typeof panel.display.table_params === 'undefined') {
            panel.display.table_params = new ngTableParams({
              page: 1,            // show first page
              count: 10,          // count per page
            }, {
              total: series[0].data.length, // length of data
              counts: [], // disable the page size toggler
              getData: function($defer, params) {
                // use built-in angular filter
                var orderedData = params.sorting() ?
                                  $filter('orderBy')(params.series[0].data, params.orderBy()) :
                                  params.series[0].data;
                params.total(params.series[0].data.length);
                $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
              }
            });
            // Pointer for getData
            panel.display.table_params.series = series;
          }
          else {
            panel.display.table_params.series = series;
            panel.display.table_params.reload();
          }
        }
        else {
          throw "Invalid display type";
        }
        panel.cache.series = series;
        console.log('this works');
      })
      .error(function(data, status, headers, config) {
        // TODO(marcua): display error.
        console.log(data);
      })
      .finally(function() {
        panel.cache.loading = false;
      });
  }
  $scope.downloadCSV = function (panel, event) {
    var csv = []; // CSV represented as 2D array
    var headerString = 'data:text/csv;charset=utf-8,';
    
    try {
      var data = panel.cache.data.events;
      if (!data.length) {
        throw "No events";
      }
    } catch (e) {
      event.target.href = headerString;
      return;
    }

    // Create line for titles
    var titles = Object.keys(data[0]);
    csv.push([]);
    for (var title in titles) {
      csv[0].push(titles[title]);
    }

    // Add all dictionary values
    for (var i in data) {
      var row = data[i];
      var new_row = [];
      for (var j in row) {
        var point = row[j];
        new_row.push(point);
      }
      csv.push(new_row);
    }

    var csvString = '';

    for (var i in csv) {
      var row = csv[i];
      for (var j in row) {
        var cell = row[j] === null ? '' : row[j].toString();
        var result = cell.replace(/"/g, '""');
        if (result.search(/("|,|\n)/g) >= 0) {
          result = '"' + result + '"';
        }
        if (j > 0) {
          csvString += ',';
        }
        csvString += result;
      }
      csvString += '\n';
    }

    event.target.href = headerString + encodeURIComponent(csvString);
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
