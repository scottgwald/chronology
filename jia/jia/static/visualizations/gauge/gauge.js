var module = angular.module('gauge', ['angular-rickshaw']);

module.factory('gauge', function () {

  var info = {
    title: 'gauge',
    readableTitle: 'Gauge',
    template: 'gauge.html',

    requiredFields: [
      '@value'
    ]
  };

  var visualization = function () {
    this.info = info;
    this.value = 0;
    
    this.setData = function (data, msg) {
      this.value = data.events[data.events.length - 1]['@value'];
    }
  }

  return {
    info: info,
    visualization: visualization
  }
});