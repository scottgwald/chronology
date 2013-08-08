if (typeof(Jia) === "undefined") {
  var Jia = {};
}

function populateProperties() {
  var stream = $("#stream-name").val();
  var $checkboxes = $("#properties-checkboxes > .controls").html("");

  if (_.has(Jia.streams, stream)) {
    var box = _.template($("#property-checkbox").html());
    _.each(Jia.streams[stream], function(property) {
      $checkboxes.append(box({value: property}));
    })
  }
}

$(function() {
  $("#new-vis-form").submit(function(event) {
    event.preventDefault();
    event.stopPropagation();

    var type = $("input[name=vistype]:checked").val();
    var stream = $("#stream-name").val();
    var start = $("#start-time").val();
    var end = $("#end-time").val();
    var properties = _.pluck($("#properties:checked"), 'value');

    var model = createNewVisualization(type, stream, start, end, properties);
    if (model) {
      Jia.router.navigate(Backbone.history.getHash() + "/" + model.get("hash"));
    }

    return false;
  });

  $("#stream-name").change(populateProperties);

  $.ajax({
    type: "GET",
    url: "/streams",
    dataType: "json",
    success: function(data) {
      Jia.streams = {};
      _.each(data['streams'], function(stream) {
        Jia.streams[stream[0]] = stream[1];
      });

      $("#stream-name").autocomplete({
        source: _.keys(Jia.streams),
        close: populateProperties
      });
    },
    error: function(data) {
      console.log("Failed to fetch stream names from Jia server.");
    }
  });

  Visualizations = new Jia.VisCollection;
  Jia.main = new Jia.MainView({collection : Visualizations});
});

