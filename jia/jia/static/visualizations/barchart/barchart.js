var module = angular.module('jia.vis.barchart', ['angular-c3']);

module.factory('barchart', function () {

  var meta = {
    title: 'barchart',
    readableTitle: 'Bar Chart',
    template: 'barchart.html',

    css: [
      '/static/visualizations/barchart/barchart.css'
    ],

    requiredFields: [
      '@label',
      '@value'
    ],

    optionalFields: [
      '@group'
    ]
  };

  var visualization = function () {
    this.meta = meta;
    this.data = [];
    this.c3data = {
      columns: []
    };

    this.setData = function (data, msg) {
      this.data = data;
      var error = false;

      // In the future, this may be an option in the query builder
      var stacked = true;

      var groups = [];
      var series = _.groupBy(data.events, function(event) {
        return event['@group'] || '';
      });

      var categories = [];
      if (_.size(series) > 0) {
        var i = 0;
        series = _.map(series, function(events, seriesName) {
          var cats = [];

          // Extract the values from the events and build a list
          var data = _.map(events, function(event) {
            // Build a list of categories (x axis) and check for duplicates
            if (i == 0 && _.contains(cats, event['@label'])) {
              msg.warn('Duplicate label "' + event['@label'] + '"');
            }
            cats.push(event['@label']);
            return event['@value'];
          });
          
          // On first iteration, save the categories list
          if (i == 0) {
            categories = cats;
          }
          // On all other iterations, check to make sure the categories
          // are consistent
          else {
            if (!_.isEqual(categories, cats)) {
              msg.error("All groups must have the same labels");
              error = true;
              return [];
            }
          }

          i++;

          // C3 expects the group name to be the first item in the array
          // followed by all the data points
          data.unshift(events[0]['@group']);

          if (stacked) {
            groups.push(events[0]['@group']);
          }

          return data;
        });
      } else {
        series = [];
        msg.warn("Data contains no events");
      }

      var cols = [];

      // If there is an error, do not display the half calculated data
      if(!error) {
        cols = series;
      }
      
      this.c3data = {
        columns: cols,
        type: 'bar',
        groups: [groups]
      };

      this.c3axis = {
        x: {
          type: 'category',
          categories: categories
        }
      }
    }
  }

  return {
    meta: meta,
    visualization: visualization
  }
});
