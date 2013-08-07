function makeGraph(data, $element) {
// data[seriesName] = [{x,y}, ...]

  var palette = new Rickshaw.Color.Palette({scheme: "munin"});

  var series = new Array();
  _.each(data, function(points, name) {
      series.push({
          name: name,
          data: points,
          color: palette.color(),
      });
  });

  Rickshaw.Series.zeroFill(series);

  var graph = new Rickshaw.Graph({
      element : $element.find(".plot")[0],
      interpolation : "linear",
      //width   : 
      height  : 350,
      series  : series,
      renderer: "area",
      min     : 0,
  });
  graph.render();

  var legend = new Rickshaw.Graph.Legend({
      graph : graph,
      element : $element.find(".legend")[0],
  });

  var shelving = new Rickshaw.Graph.Behavior.Series.Toggle({
      graph : graph,
      legend : legend,
  });

  var highlighter = new Rickshaw.Graph.Behavior.Series.Highlight({
      graph : graph,
      legend : legend,
  });

  var hoverdetail = new Rickshaw.Graph.HoverDetail({graph: graph});

  var xaxis = new Rickshaw.Graph.Axis.Time({graph: graph});
  xaxis.render();

  var yaxis = new Rickshaw.Graph.Axis.Y({
      graph : graph,
      orientation : 'left',
      element: $element.find(".y_axis")[0],
      tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
  }); 
  yaxis.render();
}

$.tablesorter.addParser({
  id: "jia-time",
  is: function(s, table, cell) {
    return false;
  },
  format: function(s, table, cell, cellIndex) {
    return Date.parse(s).getTime();
  },
  type: "numeric"
});


function makeTable(data, element) {
  var $table = $(element.find("table")[0]);
  var $header = $($table.find("thead > tr")[0]);

  $header.append("<th class='sorter-jia-time'>Time</th>");
  var row_template = "<tr><td><%=time%></td>";

  _.each(_.keys(data), function(column) {
    $header.append("<th>"+column+"</th>");
    row_template += "<td><%=data['"+column+"']%></td>";
  });

  row_template += "</tr>";
  row_template = _.template(row_template);

  var tablified_data = {};
  _.each(data, function(points, column) {
    _.each(points, function(value) {
      var time = value.x;
      tablified_data[time] = tablified_data[time] || {};
      tablified_data[time][column] = value.y;
    });
  });

  var $tbody = $(element.find("tbody")[0]);
  _.each(tablified_data, function(values, time) {
    $tbody.append(row_template({
      time: (new Date(1000 * time)).toDateString(),
      data: values
    }));
  });

  $table.tablesorter({
    sortList: [[0,1]],
    widgets: ["zebra"]
  });
}

Date.prototype.getKronosTime = function() {
  return this.getTime() * 10000;
}

function createNewVisualization(type, stream, start_time, end_time) {
  // TODO(meelap) echo errors back to the user.
  if (type != "plot" && type != "table") {
    console.log("No visualization type chosen.");
    return false;
  }
 
  start = Date.parse(start_time);
  if (!start instanceof Date) {
    console.log("Invalid start time.");
    return false;
  }

  end = Date.parse(end_time);
  if (!end instanceof Date) {
    console.log("Invalid end time.");
    return false;
  }

  if (!_.isString(stream) || stream == "") {
    console.log("Invalid stream name.");
    return false;
  }

  var model = new Jia.VisModel({
    type: type,
    title: stream,
    start: start_time,
    end: end_time
  });

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
        model.set("data", data);
        Jia.main.collection.add(model);
      }
    },
    error: function() {
      // TODO(meelap): retry, then show user the error
      console.log("Failed to fetch data points from Jia server.");
    }
  });
  return model;
}

