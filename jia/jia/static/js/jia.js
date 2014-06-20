var jia = angular.module('jia', [
  'ngRoute',
  'jia.boards'
]);

jia.config(['$routeProvider',
  function ($routeProvider) {
    $routeProvider.
      when('/boards/:boardId', {
        templateUrl: '/static/partials/board.html',
        controller: 'BoardController'
      }).
      otherwise({
        redirectTo: '/boards/new'
      });
  }]);
