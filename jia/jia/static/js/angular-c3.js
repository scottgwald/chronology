angular.module('angular-c3', [])
  .directive('c3chart', function($compile) {
    return {
      restrict: 'EA',
      scope: {
        data: '=',
        axis: '='
      },
      link: function(scope, element, attrs) {
        function update () {
          element = angular.element(element);
          element.empty();
          var container = $compile('<div></div>')(scope);
          element.append(container);

          var chart = c3.generate({
            bindto: container[0],
            data: scope.data,
            axis: scope.axis
          });
        }
        
        scope.$watch('data', function (val, prev) {
          if (!angular.equals(val, prev)) {
            update();
          }
        });

        update();
      }
    };
  });
