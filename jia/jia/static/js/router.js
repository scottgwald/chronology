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
    var pieces = view.split("&");
    var params = pieces[0].split(",");

    if (params.length != 4) {
      console.log("Can't parse bookmarked view:"+view);
      return;
    }

    var type = params[0];
    var stream = params[1];
    var start = params[2];
    var end = params[3];
    var properties = (pieces[1] && pieces[1].split(",")) || [];

    createNewVisualization(type, stream, start, end, properties);
  });
});

Backbone.history.start();
