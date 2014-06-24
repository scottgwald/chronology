var app = angular.module('jia.boards', ['ngSanitize',
                                        'ui.codemirror',
                                        'ui.bootstrap',
                                        'jia.timeseries',
                                        'jia.table',
                                        'jia.gauge',
                                        'jia.barchart'
                                       ]);

app.config(['$interpolateProvider', function($interpolateProvider) {
  // Using {[ ]} to avoid collision with server-side {{ }}.
  $interpolateProvider.startSymbol('{[');
  $interpolateProvider.endSymbol(']}');
}]);

// Add data to acceptable hrefs for CSV to be generated client side
app.config(['$compileProvider', function($compileProvider) {
  $compileProvider.aHrefSanitizationWhitelist(/^\s*(https?|ftp|mailto|data):/);
}]);

app.controller('BoardController',
['$scope', '$http', '$location', '$timeout', '$injector', '$routeParams',
 '$sce', '$sanitize', '$modal',
function ($scope, $http, $location, $timeout, $injector, $routeParams,
          $sce, $sanitize, $modal) {
  // TODO(marcua): Re-add the sweet periodic UI refresh logic I cut
  // out of @usmanm's code in the Angular rewrite.
  $scope.boardId = $routeParams.boardId;

  $scope.editorOptions = {
    lineWrapping: true,
    lineNumbers: true,
    mode: 'python',
    theme: 'mdn-like',
  };

  this.loadVisualizations = function () {
    var visualizations = {};
    _.each(app.requires, function (dependency) {
      if (dependency.indexOf('jia.') == 0) {
        module = dependency.substring('jia.'.length);
        visualizations[module] = $injector.get(module);
      }
    });
    return visualizations;
  };

  $scope.visualizations = this.loadVisualizations();

  $scope.log = function () {
    this.infos = [];
    this.info = function (message, code) {
      this.write(this.infos, message, code);
    };

    this.warns = [];
    this.warn = function (message, code) {
      this.write(this.warns, message, code);
    };

    this.errors = [];
    this.error = function (message, code) {
      this.write(this.errors, message, code);
    };

    this.write = function (log, message, code) {
      message = message.replace(/\</g, '&lt;').replace(/\>/g, '&gt;');
      message = $sanitize(message);
      if (code) {
        message = "<pre>" + message + "</pre>";
      }
      log.push($sce.trustAsHtml(message));
    }

    this.clear = function () {
      this.infos = [];
      this.warns = [];
      this.errors = [];
    };
  };

  $scope.changeVisualization = function(panel, type) {
    // Avoid recalculating stuff if the user selects the type that is already being viewed
    if (type.meta.title != panel.display.display_type) {
      panel.cache.log.clear();
      panel.display.display_type = type.meta.title;
      panel.cache.visualizations[type.meta.title] = new type.visualization();
      panel.cache.visualization = panel.cache.visualizations[type.meta.title];
      panel.cache.visualization.setData(panel.cache.data, panel.cache.log);
    }
    panel.cache.visualizationDropdownOpen = false;
  };

  $scope.callAllSources = function() {
    _.each($scope.boardData.panels, function(panel) {
      $scope.callSource(panel);
    });
  };

  $scope.callSource = function(panel) {
    panel.cache.loading = true;
    panel.cache.log.clear();
    
    $http.post('/callsource', panel.data_source)
      .success(function(data, status, headers, config) {
        panel.cache.data = data;
        panel.cache.visualization.setData(data, panel.cache.log);
      })
      .error(function(data, status, headers, config) {
        if (status == 400) {
          panel.cache.log.error(data.message);
          panel.cache.log.error(data.data.name + ": " + data.data.message);
          var traceback = "";
          _.each(data.data.traceback, function (trace) {
            traceback += trace;
          });
          panel.cache.log.error(traceback, true);
        }
        else {
          panel.cache.log.error("Could not reach server");
        }
      })
      .finally(function() {
        panel.cache.loading = false;
      });
  };
  
  $scope.downloadCSV = function (panel, event) {
    var csv = []; // CSV represented as 2D array
    var headerString = 'data:text/csv;charset=utf-8,';
    
    try {
      var data = panel.cache.data.events;
      if (!data.length) {
        throw "No data";
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
      var newRow = [];
      for (var j in row) {
        var point = row[j];
        newRow.push(point);
      }
      csv.push(newRow);
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
  };

  $scope.cleanBoard = function () {
    // Deep copy the board data and remove the cached data.
    
    if (!$scope.boardData) {
      return undefined;
    }

    return JSON.parse(JSON.stringify($scope.boardData, function(key, value) {
      if (key === 'cache') {
        return undefined;
      }
      return value;
    }));
  }

  $scope.saveBoard = function() {
    if ($scope.boardData.title == "") {
      $scope.missingTitle = true;
      return;
    }

    var data = $scope.cleanBoard();

    // TODO(marcua): display something on save failure.
    $http.post('/board/' + $scope.boardId, data)
      .success(function(data, status, headers, config) {
        $scope.boardHasChanges = false;
        if ($scope.boardId = 'new'){
          $scope.boardId = data.id;
          $location.path('/boards/' + $scope.boardId);
        }
        $scope.getBoards();
      })
      .error(function(data, status, headers, config) {
        console.log('error!');
      });
  };

  $scope.deleteBoard = function () {
    var title = $scope.boardData.title || "this board";
    if (confirm("Are you sure you want to delete " + title + "?")) {
      $http.post('/board/' + $scope.boardId + '/delete')
        .success(function (data, status, headers, config) {
          if (data.status == 'success') {
            $scope.getBoards();
            $location.path('/boards/new');
          }
        });
    }
  }

  $scope.initPanel = function(panel) {
    panel.cache = {
      data: {events: [{'@time': 0, '@value': 0}]},
      visualizations: {},
      log: new $scope.log()
    };

    // Initialize the active visualization type
    var visualizationType = panel.display.display_type;
    var newVisualization = new $scope.visualizations[visualizationType].visualization();
    panel.cache.visualizations[visualizationType] = newVisualization;
    panel.cache.visualization = panel.cache.visualizations[visualizationType];

    // Flag to toggle bootstrap dropdown menu status
    panel.cache.visualizationDropdownOpen = false;
  };

  $scope.addPanel = function() {
    var panel = {
      title: '',
      data_source: {
        source_type: 'pycode',
        refresh_seconds: null,
        code: ''
      },
      display: {
        display_type: 'timeseries'
      }
    };
    $scope.boardData.panels.unshift(panel);
    $scope.initPanel($scope.boardData.panels[0]);
  };

  $scope.getBoards = function () {
    $http.get('/boards')
      .success(function(data, status, headers, config) {
        $scope.boards = data.boards;
      });
  }

  $scope.getBoards();

  $scope.getStreams = function () {
    $http.get('/streams')
      .success(function(data, status, headers, config) {
        $scope.streams = data.streams;
      });
  }

  $scope.getStreams();

  $scope.showStreams = function () {
    $modal.open({
      templateUrl: '/static/partials/streams.html',
      scope: $scope
    });
  }

  $scope.$watch($scope.cleanBoard, function (newVal, oldVal) {
    // The initial setting of boardData doesn't count as a change in my books
    if (typeof newVal == 'undefined' || typeof oldVal == 'undefined') {
      return;
    }
    if (newVal.title != oldVal.title && newVal.title != '') {
      $scope.missingTitle = false;
    }
    if (!$scope.boardHasChanges && newVal != oldVal) {
      $scope.boardHasChanges = true;
    }
  }, true); // Deep watch

  if ($scope.boardId != 'new') {
    $http.get('/board/' + $scope.boardId)
      .success(function(data, status, headers, config) {
        angular.forEach(data.panels, function(panel) {
          $scope.initPanel(panel);
        });
        $scope.boardData = data;
      })
      .error(function(data, status, headers, config) {
        if (status == 404) {
          $location.path('/boards/new');
        }
      });
  }
  else {
    $scope.boardData = {
      title: '',
      panels: []
    };
  }

  var leavingPageText = "Anything not saved will be lost.";

  window.onbeforeunload = function () {
    if ($scope.boardHasChanges){
      return leavingPageText;
    }
  }

  $scope.$on('$destroy', function () {
    window.onbeforeunload = undefined;
  });

  $scope.$on('$locationChangeStart', function(event, next, current) {
    if($scope.boardHasChanges &&
       !confirm(leavingPageText +
                "\n\nAre you sure you want to leave this page?")) {
      event.preventDefault();
    }
  });

  Mousetrap.bind(['ctrl+s', 'meta+s'], function(e) {
    if (e.preventDefault) {
      e.preventDefault();
    } else {
      // internet explorer
      e.returnValue = false;
    }
    $scope.saveBoard();
  });
}]);

app.directive('visualization', function ($http, $compile) {
  var linker = function(scope, element, attrs) {
    scope.$watch('module', function () {
      $http.get(['static', 'visualizations', scope.module.meta.title, scope.module.meta.template].join('/'))
        .success(function(data, status, headers, config) {
          element.html(data);
          $compile(element.contents())(scope);
        });
    });
  }

  return {
    restrict: "E",
    replace: true,
    link: linker,
    scope: {
      module:'='
    }
  };
});
