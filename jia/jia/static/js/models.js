if (typeof(Jia) === "undefined") {
  var Jia = {};
}

Jia.VisModel = Backbone.Model.extend({
  defaults : {
    "type" : "plot",
    "title": "Add a new visualization",
    "start": "yesterday",
    "end"  : "today",
    "data" : null
  },
});


Jia.VisCollection = Backbone.Collection.extend({
  model : Jia.VisModel
});
