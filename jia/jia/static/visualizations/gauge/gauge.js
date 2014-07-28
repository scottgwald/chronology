var module = angular.module('jia.vis.gauge', []);

module.factory('gauge', function () {

  var meta = {
    title: 'gauge',
    readableTitle: 'Gauge',
    template: 'gauge.html',

    css: [
      '/static/visualizations/gauge/gauge.css'
    ],

    requiredFields: [
      '@value'
    ]
  };

  var visualization = function () {
    this.meta = meta;
    this.value = 0;
    
    this.setData = function (data, msg) {
      if (data.events.length > 1) {
        msg.warn("Gauge accepts one value. Only the most recent is shown.");
      }
      this.value = data.events[data.events.length - 1]['@value'];
    }
  }

  return {
    meta: meta,
    visualization: visualization
  }
});