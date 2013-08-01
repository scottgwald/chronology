if (typeof(Jia) === "undefined") {
  var Jia = {};
}

Date.prototype.getKronosTime = function() {
  return this.getTime() * 10000;
}

function createNewVisualization(type, stream, start, end) {
  // TODO(meelap) echo errors back to the user.
  if (type != "plot" && type != "table") {
    console.log("No visualization type chosen.");
    return;
  }
 
  start = Date.parse(start);
  if (!start instanceof Date) {
    console.log("Invalid start time.");
    return;
  }

  end = Date.parse(end);
  if (!end instanceof Date) {
    console.log("Invalid end time.");
    return;
  }

  if (!_.isString(stream) || stream == "") {
    console.log("Invalid stream name.");
    return;
  }

  $.ajax({
    type: "POST",
    url: "/get",
    dataType: "json",
    data: {
      stream: stream,
      start_time: start.getKronosTime(),
      end_time: end.getKronosTime(),
    },
    success: function(data) {
      if (_.has(data, "error")) {
        console.log("Server side error fetching data points:"+data);
      } else {
        var newvis = new Jia.VisModel({
          type: type,
          title: stream,
          data: data
        });
        Jia.main.collection.add(newvis);
      }
    },
    error: function() {
      // TODO(meelap): retry, then show user the error
      console.log("Failed to fetch data points from Jia server.");
    }
  });
}

$(function() {
  $("#new-vis-form").submit(function(event) {
    event.preventDefault();
    event.stopPropagation();

    var type = $("input[name=vistype]:checked").val();
    var stream = $("#stream-name").val();
    var start = $("#start-time").val();
    var end = $("#end-time").val();
    createNewVisualization(type, stream, start, end);

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

