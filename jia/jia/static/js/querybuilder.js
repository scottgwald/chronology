var qb = angular.module('jia.querybuilder', []);

qb.controller('QueryBuilderController', ['$scope', function($scope) {
  $scope.query = [];
  $scope.nextStep = null;

  $scope.$watch('nextStep', function (newVal, oldVal) {
    console.log('nextStep', newVal, oldVal)
    if (newVal) {
      var newnew = jQuery.extend(true, {}, newVal);
      $scope.query.push({'operator': newnew});
      console.log('setting nextstep null');
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

qb.directive('querybuilder', function ($http, $compile) {
  return {
    restrict: "E",
    templateUrl: '/static/partials/querybuilder.html'
  };
});

qb.directive('operator', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.$watch('operator', function (newVal, oldVal) {
      console.log('new', scope.newop);
      if (!scope.newop && typeof newVal != 'undefined') {
        $http.get(['static', 'partials', 'operators', scope.operator.value + '.html'].join('/'))
          .success(function(data, status, headers, config) {
            $(element).find('div.args').html(data);
            $compile(element.contents())(scope);
          });
      }
    });
  }

  var controller = ['$scope', function($scope) {
    $scope.operators = [
      {name: 'Transform', value: 'transform'},
      {name: 'Filter', value: 'filter'},
      {name: 'Order by', value: 'orderby'},
      {name: 'Limit', value: 'limit'},
      {name: 'Aggregate', value: 'aggregate'},
    ];
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operator.html',
    controller: controller,
    link: linker,
    scope: {
      operator: '=',
      newop: '='
    }
  };
});

qb.directive('newoperator', function ($http, $compile) {
  return {
    restrict: "E",
    templateUrl: '/static/partials/operator.html',
    scope: {
      operator: '='
    }
  };
});

qb.directive('cpf', function ($http, $compile) {
  var controller = ['$scope', function ($scope) {
    $scope.functions = [
      {name: 'Ceiling', args: ['Property', 'Base', 'Offset']},
      {name: 'Floor', args: ['Property', 'Base', 'Offset']},
      {name: 'Date Truncate', args: ['Property', 'Time scale']},
      {name: 'Date Part', args: ['Property', 'Time scale']},
      {name: 'Lowercase', args: ['Property']},
      {name: 'Uppercase', args: ['Property']},
      {name: 'Random Integer', args: ['Low', 'High']}
      /* 'Add': ['Property 1', 'Property 2'], */
      /* 'Subtract': ['Property 1', 'Property 2'], */
      /* 'Length': ['?'], */
    ];
    $scope.func = $scope.functions[0];

    $scope.types = [
      {name: 'Property'},
      {name: 'Constant'},
      {name: 'Function'}
    ];
    $scope.type = $scope.types[0];
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/cpf.html',
    controller: controller,
    scope: {}
  };
});

qb.directive('op', function ($http, $compile) {
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
    $scope.type = 'property';
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/op.html',
    controller: controller,
    scope: {
      value: '='
    }
  };
});