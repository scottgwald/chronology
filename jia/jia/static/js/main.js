if (typeof(Jia) === "undefined") {
  var Jia = {};
}

$(function() {
  $("#new-vis-form").submit(function(event) {
    event.preventDefault();
    event.stopPropagation();

    var type = $("input[name=vistype]:checked").val();
    var stream = $("#stream-name").val();
    var start = $("#start-time").val();
    var end = $("#end-time").val();

    var model = createNewVisualization(type, stream, start, end);
    if (model) {
      Jia.router.navigate(Backbone.history.getHash() + "/" + model.get("hash"));
    }

    return false;
  });

  $.ajax({
    type: "GET",
    url: "/streams",
    dataType: "json",
    success: function(data) {
      // data should be a list of values
      $("#stream-name").autocomplete({
        source: data['streams']
      });
    },
    error: function(data) {
      console.log("Failed to fetch stream names from Jia server.");
    }
  });

  Visualizations = new Jia.VisCollection;
  Jia.main = new Jia.MainView({collection : Visualizations});
});

