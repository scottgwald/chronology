if (typeof(Jia) === "undefined") {
  var Jia = {};
}

Jia.VisModel = Backbone.Model.extend({
  defaults : {
    "type": null,
    "stream": null,
    "properties": null,
    "start": null,
    "end": null,
    "data": null,
    "hash": null
  },

  initialize: function() {
    var hash = [this.get("type"),
                this.get("stream"),
                this.get("start"),
                this.get("end")].join();
    var properties = this.get("properties");
    if (properties) {
      hash += "&" + properties.join();
    }
    this.set("hash", hash);
  }
});


Jia.VisCollection = Backbone.Collection.extend({
  model : Jia.VisModel
});
