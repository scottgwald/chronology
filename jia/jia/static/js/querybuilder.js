var qb = angular.module('jia.querybuilder', []);

function findObjectInListBasedOnKey(list, keyName, keyVal) {
  for (var i = 0; i < list.length; i++) {
    if (list[i][keyName] == keyVal) {
      return list[i];
    }
  }
}

qb.directive('querybuilder', function ($http, $compile) {
  var controller = ['$scope', function($scope) {
    $scope.nextStep = null;

    $scope.$watch('nextStep', function (newVal, oldVal) {
      if (newVal) {
        $scope.query.push($scope.nextStep);
        $scope.nextStep = null;
      }
    });

    $scope.delete = function (step) {
      var index = $scope.query.indexOf(step);
      if (index > -1) {
        $scope.query.splice(index, 1);
      }
    };
  }];
  
  return {
    restrict: 'E',
    templateUrl: '/static/partials/querybuilder.html',
    controller: controller,
    scope: {
      query: '='
    }
  };
});

qb.directive('operator', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.$watch('operator', function (newVal, oldVal) {
      if (!scope.newop && typeof newVal != 'undefined') {
        $http.get(['static', 'partials', 'operators',
                   scope.operator.operator + '.html'].join('/'))
          .success(function(data, status, headers, config) {
            $(element).find('div.args').html(data);
            $compile(element.contents())(scope);
          });
      }
    });
  }

  var controller = ['$scope', function($scope) {
    // Initialize args lists with proper dimensions
    $scope.operators = [
      {name: 'Transform', operator: 'transform', args: []},
      {name: 'Filter', operator: 'filter', args: []},
      {name: 'Order by', operator: 'orderby', args: [[]]},
      {name: 'Limit', operator: 'limit', args: []},
      {name: 'Aggregate', operator: 'aggregate', args: [[[]], [[]]]},
    ];

    $scope.addArg = function (argList) {
      if (typeof argList != 'undefined') {
        argList.push([]);
      }
      else {
        $scope.operator.args.push([]);
      }
    };
    $scope.removeArg = function (idx, argList) {
      if (typeof argList != 'undefined') {
        console.log(argList, idx);
        argList.splice(idx, 1);
      }
      else {
        $scope.operator.args.splice(idx, 1);
      }
    }
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operator.html',
    controller: controller,
    link: linker,
    scope: {
      operator: '=',
      newop: '=',
      count: '='
    }
  };
});


qb.directive('cpf', function ($http, $compile) {
  var controller = ['$scope', function ($scope) {
    $scope.functions = [
      {
        name: 'Ceiling',
        value: 'ceiling',
        args: [],
        options: [
          {name: 'Property', type: 'property'},
          {name: 'Base', type: 'constant'},
          {name: 'Offset', type: 'constant'}
        ]
      },
      {
        name: 'Floor',
        value: 'floor',
        args: [],
        options: [
          {name: 'Property', type: 'property'},
          {name: 'Base', type: 'constant'},
          {name: 'Offset', type: 'constant'}
        ]
      },
      {
        name: 'Date Truncate',
        value: 'date_trunc',
        args: [],
        options: [
          {name: 'Property', type: 'property'},
          {
            name: 'Time scale',
            type: 'constant',
            choices: [
              'second',
              'minute',
              'hour',
              'day',
              'week',
              'month',
              'year',
            ]
          }
        ]
      },
      {
        name: 'Date Part',
        value: 'date_part',
        args: [],
        options: [
          {name: 'Property', type: 'property'},
          {
            name: 'Time scale',
            type: 'constant',
            choices: [
              'second',
              'minute',
              'hour',
              'weekday',
              'day',
              'month'
            ]
          }
        ]
      },
      {
        name: 'Lowercase',
        value: 'lowercase',
        args: [],
        options: [
          {name: 'Property', type: 'property'}
        ]
      },
      {
        name: 'Uppercase',
        value: 'uppercase',
        args: [],
        options: [
          {name: 'Property', type: 'property'}
        ]
      },
      {
        name: 'Random Integer',
        value: 'rand_int',
        args: [],
        options: [
          {name: 'Low', type: 'constant'},
          {name: 'High', type: 'constant'}
        ]
      }
      /*
       * TODO(derek): Missing functions
       *
       * or make an HTTP endpoint for determining this info
       */
    ];
    $scope.func = $scope.functions[0];

    $scope.types = [
      {name: 'Property', type: 'property'},
      {name: 'Constant', type: 'constant'},
      {name: 'Function', type: 'function'}
    ];
    $scope.type = $scope.types[0];
    $scope.args = [];
        
    if (!$scope.arg || !$scope.arg.val) {
      $scope.arg = {};
    }
    else if ($scope.arg.val.cpf_type) {
      $scope.type = findObjectInListBasedOnKey($scope.types, 'type',
                                               $scope.arg.val.cpf_type);
      $scope.func = findObjectInListBasedOnKey($scope.functions, 'value',
                                               $scope.arg.val.function_name);
      _.each($scope.arg.val.function_args, function (arg, index) {
        if (typeof arg.property_name != 'undefined') {
          $scope.args.push(arg.property_name);
        }
        else if (typeof arg.constant_value != 'undefined') {
          $scope.args.push(arg.constant_value);
        }
      });
      $scope.name = $scope.arg.val.property_name;
      $scope.value = $scope.arg.val.constant_value;
    }

    $scope.$watch(function () {
      return [$scope.func,
              $scope.type,
              $scope.name,
              $scope.value,
              $scope.args];
    }, function () {
      var args = [];
      if (!$scope.type) return;
      _.each($scope.args, function (arg, index) {
        var type = $scope.func.options[index].type;
        var cpf = {
          'cpf_type': type
        };
        if (type == 'property') {
          cpf['property_name'] = arg;
        }
        else if (type == 'constant') {
          cpf['constant_value'] = arg;
        }
        args.push(cpf);
      });
      $scope.arg.val = {
        'cpf_type': $scope.type.type,
        'function_name': $scope.func.value,
        'function_args': args,
        'property_name': $scope.name,
        'constant_value': $scope.value
      };
    }, true);

  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/cpf.html',
    controller: controller,
    scope: {
      arg: '=' 
    }
  };
});

qb.directive('op', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    if (scope.arg.val) {
      var val = scope.arg.val;
      scope.type = findObjectInListBasedOnKey(scope.types, 'value', val);
    }
  };

  var controller = ['$scope', function ($scope) {
    $scope.types = [
      {name: 'is less than', value: 'lt'},
      {name: 'is less than or equal to', value: 'lte'},
      {name: 'is greater than', value: 'gt'},
      {name: 'is greater than or equal to', value: 'gte'},
      {name: 'is equal to', value: 'eq'},
      {name: 'contains', value: 'contains'},
      {name: 'is in', value: 'in'},
      {name: 'matches regex', value: 'regex'}
    ];
    $scope.type = $scope.types[0];

    if (!$scope.arg) {
      $scope.arg = {
        'val': ''
      };
    }

    $scope.$watch('type', function () {
      if ($scope.type != undefined) {
        $scope.arg.val = $scope.type.value;
      }
    });
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/op.html',
    controller: controller,
    link: linker,
    scope: {
      arg: '='
    }
  };
});

qb.directive('aggtype', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    if (scope.arg.val) {
      var val = scope.arg.val;
      scope.type = findObjectInListBasedOnKey(scope.types, 'value', val);
    }
  };

  var controller = ['$scope', function ($scope) {
    $scope.types = [
      {name: 'Minimum', value: 'min'},
      {name: 'Maximum', value: 'max'},
      {name: 'Average', value: 'avg'},
      {name: 'Count', value: 'count'},
      {name: 'Sum', value: 'sum'},
      {name: 'Value count', value: 'valuecount'}
    ];
    $scope.type = $scope.types[3];

    if (!$scope.arg) {
      $scope.arg = {
        'val': ''
      };
    }

    $scope.$watch('type', function () {
      if ($scope.type != undefined) {
        $scope.arg.val = $scope.type.value;
      }
    });
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/op.html',
    controller: controller,
    link: linker,
    scope: {
      arg: '='
    }
  };
});

qb.directive('val', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.val = scope.arg.val;
  };

  var controller = ['$scope', function ($scope) {
    $scope.$watch('val', function () {
      $scope.arg.val = $scope.val;
    });

    if (!$scope.arg) {
      $scope.arg = {};
    }
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/input.html',
    controller: controller,
    link: linker,
    scope: {
      placeholder: '=?',
      arg: '='
    }
  };
});

qb.directive('prop', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    if (scope.arg) {
      scope.val = scope.arg['property_name'];
    }
  };

  var controller = ['$scope', function ($scope) {
    $scope.placeholder = 'Property';
    $scope.$watch('val', function () {
      $scope.arg = {
        'cpf_type': 'property',
        'property_name': $scope.val
      }
    });
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/input.html',
    controller: controller,
    link: linker,
    scope: {
      arg: '=',
      placeholder: '@'
    }
  };
});

