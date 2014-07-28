var module = angular.module('jia.vis.timeseries', ['angular-rickshaw']);

module.factory('timeseries', function () {

  var meta = {
    title: 'timeseries',
    readableTitle: 'Time Series',
    template: 'timeseries.html',

    css: [
      "//cdnjs.cloudflare.com/ajax/libs/rickshaw/1.4.6/rickshaw.min.css",
    ],

    js: [
      "//cdnjs.cloudflare.com/ajax/libs/d3/3.4.1/d3.min.js",
      "//cdnjs.cloudflare.com/ajax/libs/rickshaw/1.4.6/rickshaw.min.js",
      "//ngyewch.github.io/angular-rickshaw/rickshaw.js"
    ],

    requiredFields: [
      '@time',
      '@value'
    ],

    optionalFields: [
      '@group'
    ]
  };

  var visualization = function () {

    this.meta = meta;
    this.series = [{name: 'series', data: [{x: 0, y: 0}]}];

    this.timeseriesOptions = {
      renderer: 'line',
      width: parseInt($('.panel').width() * .73)
    };

    this.timeseriesFeatures = {
      palette: 'spectrum14',
      xAxis: {},
      yAxis: {},
      hover: {},
    };
    
    this.setData = function (data, msg) {
      // `data` should contain an `events` property, which is a list
      // of Kronos-like events.  The events should be sorted in
      // ascending time order.  An event has at least two fields `@time`
      // (Kronos time: 100s of nanoseconds since the epoch), and
      // `@value`, a floating point value.  An optional `@group`
      // attribute will split the event stream into different
      // groups/series.  All events in the same `@group` will be
      // plotted on their own line.

      // TODO(marcua): do a better job of resizing the plot given the
      // legend size.
      this.timeseriesOptions.width = parseInt($('.panel').width() * .73);

      var series = _.groupBy(data.events, function(event) {
        return event['@group'] || 'series';
      });
      delete this.timeseriesFeatures.legend;

      if (_.size(series) > 0) {
        series = _.map(series, function(events, seriesName) {
          return {name: seriesName, data: _.map(events, function(event) {
            return {x: event['@time'] * 1e-7, y: event['@value']};
          })}
        });
        if (_.size(series) > 1) {
          this.timeseriesFeatures.legend = {toggle: true, highlight: true};
        }
      } else {
        series = [{name: 'series', data: [{x: 0, y: 0}]}];
        msg.warn("Data contains no events");
      }

      this.series = series;
    }
  }

  return {
    meta: meta,
    visualization: visualization
  }
});