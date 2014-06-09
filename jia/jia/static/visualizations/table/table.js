var module = angular.module('table', ['ngTable']);

module.factory('table', function ($filter, ngTableParams) {

  var info = {
    title: 'table',
    readableTitle: 'Table',
    template: 'table.html',

    optionalFields: [
      '@time',
      '@group'
    ]
  }

  var visualization = function () {

    this.info = info;
    this.data = [];

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
              data[key.hashCode()] = Date(event[key] * 1e-7);
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
  }

  return {
    info: info,
    visualization: visualization
  }

});