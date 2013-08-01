if (typeof(Jia) === "undefined") {
  var Jia = {};
}

Jia.VisModel = Backbone.Model.extend({
  defaults : {
    "type" : "plot",
    "title": "Add a new visualization",
    "start": "yesterday",
    "end"  : "today",
    "data" : null,
    "hash" : null
  },

  initialize: function() {
    var hash = [this.get("type"),
                this.get("title"),
                this.get("start"),
                this.get("end")].join();
    this.set("hash", hash);
  }
});


Jia.VisCollection = Backbone.Collection.extend({
  model : Jia.VisModel
});
