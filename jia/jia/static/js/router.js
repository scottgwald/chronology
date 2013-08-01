if (typeof(Jia) === "undefined") {
  var Jia = {};
}

Jia.Router = Backbone.Router.extend({
  routes: {
    "*views": "makeVisualization",
  }
});

Jia.router = new Jia.Router;

Jia.router.on("route:makeVisualization", function(views) {
  views = (views && views.split("/")) || [];

  _.each($("#visualizations > li"), function(el) {
    el = $(el);
    var index = _.indexOf(views, el.attr("id"));
    if (index != -1) {
      views.splice(index, 1);
    } else {
      el.remove();
    }
  });

  _.each(views, function(view) {
    var params = view.split(",");
    if (params.length != 4) {
      console.log("What is this:"+view);
      return;
    }

    var type = params[0];
    var stream = params[1];
    var start = params[2];
    var end = params[3];
    createNewVisualization(type, stream, start, end);
  });
});

Backbone.history.start();
