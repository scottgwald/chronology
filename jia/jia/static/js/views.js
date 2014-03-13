// Make Underscore.js templates work with Handlebars.js template syntax.
_.templateSettings = {
  evaluate:    /\{\{=(.+?)\}\}/g,
  interpolate: /\{\{(.+?)\}\}/g,
  escape:      /\{\{-(.+?)\}\}/g
};

// Helper for re-rendering sub-views.
Backbone.View.prototype.assign = function (selector, view) {
  var selectors;
  if (_.isObject(selector)) {
    selectors = selector;
  } else {
    selectors = {};
    selectors[selector] = view;
  }
  if (!selectors) return;
  _.each(selectors, function (view, selector) {
    view.setElement(this.$(selector)).render();
  }, this);
}

var TimeSeriesView = Backbone.View.extend({
  tagName: 'div',
  className: 'timeseries',

  initialize: function(options) {
    this.listenTo(this.model, 'change:events', this.render);
    this.listenTo(this.model.get('events'), 'add remove reset', this.render);

    this.yLines = options.yLines || [{name: 'value', key: '@value'}];
    this.series = {};

    // Initialize all series arrays.
    _.each(this.yLines, function(yLine) {
      this.series[yLine.name] = [];
    }, this);

  },

  renderGraph: function() {
    // Reset all series arrays.
    _.each(this.yLines, function(yLine) {
      this.series[yLine.name].length = 0;
    }, this);

    this.model.get('events').forEach(function(event) {
      var x = event.get('@time').toSeconds();
      _.each(this.yLines, function(yLine) {
        this.series[yLine.name].push({x: x, y: event.get(yLine.key) || 0});
      }, this);
    }, this);

    if (this._graph) {
      this._graph.update();
      return;
    }
    
    this.$el.empty();

    var graph = new Rickshaw.Graph({
      element: this.el,
      interpolation: 'linear',
      renderer: 'line',
      series: _.map(this.yLines, function(yLine) {
        if (!this.series[yLine.name].length) {
          this.series[yLine.name].push({x: 0, y: 0});
        }
        return {
          data: this.series[yLine.name],
          color: 'steelblue',
          name: yLine.name
        };
      }, this)
    });
    graph.render();
    this._graph = graph;

    var hoverDetail = new Rickshaw.Graph.HoverDetail({
      graph: graph
    });

    var xAxis = new Rickshaw.Graph.Axis.Time({
      graph: graph
    });
    xAxis.render();
  
    var yAxis = new Rickshaw.Graph.Axis.Y({
      graph: graph,
      orientation: 'right',
      tickFormat: Rickshaw.Fixtures.Number.formatKMBT
    });
    yAxis.render();
  },

  render: function() {
    this.renderGraph();
    return this;
  }
});

var PyCodeView = Backbone.View.extend({
  tagName: 'div',
  className: 'pycode',
  template: ('<div class="timeseries"></div> \
              <div class="code-box"> \
                <div class="label">Edit the code below:</div> \
                <textarea id="code"></textarea> \
                <button id="submit">Submit</button> \
              </div>'),
  events: {
    'click #submit': 'onSubmit'
  },

  initialize: function(options) {
    this.timeSeriesView = new TimeSeriesView({model: this.model});
  },

  onSubmit: function() {
    this.model.save();
  },

  renderCodeMirror: function() {
    var self = this;
    var pyCodeMirror = CodeMirror.fromTextArea(this.$('#code')[0],
                                               {mode: 'python',
                                                lineWrapping: true,
                                                lineNumbers: true,
                                                theme: 'mdn-like'});
    pyCodeMirror.getDoc().setValue(this.model.get('code') || '');
    pyCodeMirror.on('change', function(pyCodeMirror) {
      self.model.set('code', pyCodeMirror.getValue());      
    });
    this._pyCodeMirror = pyCodeMirror;
  },

  render: function() {
    this.$el.html(this.template);
    this.renderCodeMirror();
    this.assign('.timeseries', this.timeSeriesView);
    return this;
  }
});

var BoardView = Backbone.View.extend({
  tagName: 'div',
  className: 'board',
  template: '<div class="pycode"></div>',

  initialize: function(options) {
    this.pyCodeView = new PyCodeView({model: this.model.get('pycode')});
  },

  render: function() {
    this.$el.html(this.template);
    this.assign('.pycode', this.pyCodeView);
    return this;
  }
});

var Jia = function() {
  var self = this;

  this.run = function() {
    var model = new Board({id: location.pathname.substring(1)});
    model.fetch({
      success: function() {
        self.board = new BoardView({model: model, el: $('.board')});
        self.board.render();
      }
    });
  };
};

jia = new Jia();
jia.run();