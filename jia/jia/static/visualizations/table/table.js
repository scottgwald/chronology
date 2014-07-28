var module = angular.module('jia.vis.table', ['ngTable']);

module.factory('table', function ($filter, ngTableParams) {

  var meta = {
    title: 'table',
    readableTitle: 'Table',
    template: 'table.html',

    css: [
      '/static/css/ng-table.min.css',
    ],

    js: [
      '/static/js/ng-table.min.js',
    ],

    optionalFields: [
      '@time',
      '@group'
    ]
  };

  var visualization = function () {

    this.meta = meta;
    this.data = [];

    // Provide a hash method for strings
    // ng-table requires unique variable-like IDs for cols
    String.prototype.hashCode = function () {
      var hash = 0, i, chr, len;
      if (this.length == 0) return 'a0';
      for (i = 0, len = this.length; i < len; i++) {
        chr   = this.charCodeAt(i);
        hash  = ((hash << 5) - hash) + chr;
        hash |= 0; // Convert to 32bit integer
      }
      // Must start with a letter to make ng-table happy
      return 'a' + hash;
    };

    this.setData = function (data, msg) {
      var events = data.events;
      var series = {};
      
      if (_.size(events) > 0) {

        column_names = Object.keys(events[0]);
        cols = [];
        for (name in column_names) {
          cols.push({title: column_names[name], field: column_names[name].hashCode()});
        }

        series = {name: 'events', cols: cols, data: _.map(events, function(event) {
          data = {};
          Object.keys(event).forEach(function (key) {
            if (key == '@time') {
              var date = new Date(event[key] * 1e-4);
              var dateString = date.toLocaleDateString() + " " + date.toLocaleTimeString();
              data[key.hashCode()] = dateString;
            }
            else {
              data[key.hashCode()] = event[key];
            }
          });
          return data;
        })};

      } else {
        series = {name: 'events', cols: [], data: []};
      }

      if (typeof this.tableParams === 'undefined') {
        this.tableParams = new ngTableParams({
          page: 1,            // show first page
          count: 10,          // count per page
        }, {
          total: series.data.length, // length of data
          counts: [], // disable the page size toggler
          getData: function($defer, params) {
            // use built-in angular filter
            var orderedData = params.sorting() ?
                              $filter('orderBy')(params.series.data, params.orderBy()) :
                              params.series.data;
            params.total(params.series.data.length);
            $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
          }
        });
        // Pointer for getData
        this.tableParams.series = series;
      }
      else {
        this.tableParams.series = series;
        this.tableParams.reload();
      }
    }
  };

  return {
    meta: meta,
    visualization: visualization
  };

});
