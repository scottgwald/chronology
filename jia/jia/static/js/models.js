var toJSON = Backbone.Model.prototype.toJSON;

// Serialize Backbone.Model values to their ID attribute.
Backbone.Model.prototype.toJSON = function(options) {
  var json = toJSON.call(this, options);
  _.each(json, function(value, key) {
    if (value instanceof Backbone.Model) {
      json[key] = value.id;
    }
  });
  return json;
};

var Event = Backbone.Model.extend({
  /*
    @time (Date)
    ...
  */

  parse: function(response, options) {
    response = _.clone(response);
    response['@time'] = Date.fromKronosTime(response['@time']);
    return response;
  }
});

var EventCollection = Backbone.Collection.extend({
  model: Event,
  comparator: '@time'
});

var PyCode = Backbone.Model.extend({
  /*
    board (Board)
    code (String)
    events (EventCollection)
  */

  urlRoot: '/pycode',

  initialize: function(options) {
    if (!options.events) {
      this.set('events', new EventCollection());
    }
  },

  parse: function(response, options) {
    response = _.clone(response);
    delete response.board; // PyCodes can't be moved between boards.
    response.events = new EventCollection(response.events, {parse: true});
    return response;
  },

  toJSON: function(options) {
    var json = Backbone.Model.prototype.toJSON.call(this);
    delete json.events;
    return json;
  }
});

var Board = Backbone.Model.extend({
  /*
    pycode (PyCode)
  */

  urlRoot: '/board',

  parse: function(response, options) {
    response = _.clone(response);
    var pycode = response.pycode;
    pycode.board = this;
    response.pycode = new PyCode(pycode);
    if (!response.pycode.id) {
      response.pycode.save();
    } else {
      response.pycode.fetch();
    }
    return response;
  },

  toJSON: function(options) {
    return {id: this.id};
  }
});