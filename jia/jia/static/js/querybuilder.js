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
}]);

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
    $scope.operators = [
      {name: 'Transform', operator: 'transform', args: []},
      {name: 'Filter', operator: 'filter', args: []},
      {name: 'Order by', operator: 'orderby', args: [[]]},
      {name: 'Limit', operator: 'limit', args: []},
      {name: 'Aggregate', operator: 'aggregate', args: [[]]},
    ];

    $scope.addArg = function () {
      $scope.operator.args.push([]);
    };
    $scope.removeArg = function (idx) {
      $scope.operator.args.pop(idx);
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
  var linker = function (scope, element, attrs) {
    if (scope.arg.val) {
      var type = scope.arg.val['cpf_type'];
      var func = scope.arg.val['function_name'];
      var name = scope.arg.val['property_name'];
      var val = scope.arg.val['constant_value'];
      var args = scope.arg.val['function_args'];
      scope.type = findObjectInListBasedOnKey(scope.types, 'type', type);
      scope.func = findObjectInListBasedOnKey(scope.functions, 'value', func);
      scope.name = name;
      scope.value = val;
      var strippedArgs = [];
      _.each(args, function (arg) {
        if (arg.cpf_type == 'property') {
          strippedArgs.push(arg.property_name);
        }
        else if (arg.cpf_type == 'constant') {
          strippedArgs.push(arg.constant_value);
        }
      });
      scope.args = strippedArgs;
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
        value: 'datetrunc',
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
        value: 'datepart',
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
        value: 'randint',
        args: [],
        options: [
          {name: 'Low', type: 'constant'},
          {name: 'High', type: 'constant'}
        ]
      }
      /*
       * TODO(derek): Missing functions
       */
    ];
    $scope.func = $scope.functions[0];

    if (!$scope.arg) {
      $scope.arg = {};
    }

    $scope.types = [
      {name: 'Property'},
      {name: 'Constant'},
      {name: 'Function'}
    ];
    $scope.type = $scope.types[0];
    $scope.args = [];
        
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
    link: linker,
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
      $scope.arg = {};
    }

    $scope.$watch('type', function () {
      $scope.arg.val = $scope.type.value;
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
      {name: 'is less than'},
      {name: 'is less than or equal to'},
      {name: 'is greater than'},
      {name: 'is greater than or equal to'},
      {name: 'is equal to'},
      {name: 'contains'},
      {name: 'is in'},
      {name: 'matches regex'}
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
      arg: '='
    }
  };
});

