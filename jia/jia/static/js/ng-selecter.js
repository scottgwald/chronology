/*
 * Simple Angular directive for Ben Plum's Selecter.js
 *
 * In controller:
 *
 * $scope.choices = [
 *   {name: 'Option 1', anythingElse: 39353},
 *   {name: 'Option 2', selected: true, anythingElse: 40406},
 *   {name: 'Option 3', anythingElse: 35353}
 * ]
 * 
 * // Alternatively, you can make optgroups:
 * $scope.choices = {
 *   'Group 1': [
 *     {name: 'Option 1', anythingElse: 39353},
 *     {name: 'Option 2', anythingElse: 40406},
 *     {name: 'Option 3', anythingElse: 35353}
 *   ],
 *   'Group 2': [
 *     {name: 'Option 4', anythingElse: 39353},
 *     {name: 'Option 5', anythingElse: 40406},
 *     {name: 'Option 6', anythingElse: 35353}
 *   ]
 * }
 * 
 * $scope.chosen = $scope.choices[0];
 *
 * $scope.enabled = true;
 * 
 * $scope.optionalConfig = {
 *   customClass: 'fancy',
 *   label: 'Choose an option...'
 * }
 * 
 * In view:
 * <selecter model="chosen"
 *           options="choices"
 *           disabled="false"
 *           config="optionalConfig">
 * </selecter>
 *
 */

angular.module('selecter', [])

.directive('selecter', ['$http', '$compile', function ($http, $compile) {
  var linker = function(scope, element, attrs) {
    var changeCallback = function (value, index) {
      scope.model = lookupOption(value);
      scope.$apply();
    }

    var lookupOption = function (value) {
      var options;
      if (scope.options && typeof scope.options.length != 'undefined') {
        optGroups = false;
        options = {
          options: scope.options
        };
      }
      else {
        options = scope.options;
      }
      for (var key in options) {
        if (options.hasOwnProperty(key)) {
          for (var i = 0; i < options[key].length; i++) {
            if (options[key][i].name == value) {
              return options[key][i];
            }
          }
        }
      }
    }

    var createSelecter = function (selectedOption) {
      $(element).html('');
      var select = $('<select></select>');
      var optGroups = true;
      var options;
      
      // if (typeof selectedOption == 'undefined' &&
      //     $(element).find('option:selected').length) {
      //   selectedOption = $(element).find('option:selected').text();
      // }

      if (scope.disabled) {
        select.attr('disabled', 'disabled');
      }
      $(element).append(select);
      
      if (scope.options && typeof scope.options.length != 'undefined') {
        optGroups = false;
        options = {
          options: scope.options
        };
      }
      else {
        options = scope.options;
      }

      for (var key in options) {
        if (options.hasOwnProperty(key)) {
          var appendTo;
          if (optGroups) {
            appendTo = $('<optgroup></optgroup>').attr('label', key);
            select.append(appendTo);
          }
          else {
            appendTo = select;
          }
          for (var i = 0; i < options[key].length; i++) {
            var option = $('<option></option>');
            var optionAttrs = options[key][i];
            option.text(optionAttrs.name);
            option.attr('value', optionAttrs.name);
            if (optionAttrs.disabled) {
              option.attr('disabled', 'disabled');
            }
            if (selectedOption &&
                selectedOption.name == optionAttrs.name) {
              option.attr('selected', 'selected');
            }
            appendTo.append(option);
          }
        }
      }
      if (typeof scope.config == 'object') {
        scope.config.callback = changeCallback;
      }
      else {
        scope.config = {
          callback: changeCallback
        }
      }
      select.selecter(scope.config);
    }
     
    var updateSelector = function (newVal) {
      // Update the selecter when the value changes in scope
      // Selecter doesn't provide an update method, so destroy and recreate
      $(element).find('select').selecter('destroy');
      createSelecter(newVal);
    };
 
    scope.$watch('model', function (newVal, oldVal) {
      // The timeout of zero is magic to wait for an ng-repeat to finish
      // populating the <select>. See: http://stackoverflow.com/q/12240639/
      console.log('model', newVal, oldVal, scope.sid);
      setTimeout(function () { updateSelector(newVal); });
    });

    // scope.$watch('options', function (newVal, oldVal) {
    //   setTimeout(function () { updateSelector(); });
    // }, true);

    scope.$on('$destroy', function() {
      $(element).find('select').selecter('destroy');
    });
  }

  return {
    restrict: "E",
    replace: false,
    link: linker,
    scope: {
      model: '=',
      options: '=',
      config: '=?',
      disabled: '=?',
      sid: '=?'
    }
  };
}]);