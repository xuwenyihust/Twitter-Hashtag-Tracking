//////////////////////////////////////////////////////////////////
//
// Keyword Chart
//
// set the dimensions of the canvas

var margin = {top: 20, right: 100, bottom: 70, left: 60},
    width = 400 - margin.left - margin.right,
    height = 300 - margin.top - margin.bottom;

// set the ranges
var x = d3.scale.ordinal().rangeRoundBands([0, width], .05);
var y = d3.scale.linear().range([height, 0]);
// define the axis
var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom")
var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .ticks(10);
// add the SVG element
var keyword = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform", 
          "translate(" + margin.left + "," + margin.top + ")");
// load the data
d3.json("/data/keywords", function(error, data) {
    data.forEach(function(d) {
        d.Keyword = d.Keyword;
        d.Count = +d.Count;
    });
  // scale the range of the data
  x.domain(data.map(function(d) { return d.Keyword; }));
  y.domain([0, d3.max(data, function(d) { return d.Count; })]);
  // add axis
  keyword.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis)
    .selectAll("text")
      .style("text-anchor", "end")
      .attr("dx", "-.8em")
      .attr("dy", "-.55em")
      .attr("transform", "rotate(-90)" );
  keyword.append("g")
      .attr("class", "y axis")
      .call(yAxis)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 5)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Frequency");
  // Add bar chart
  keyword.selectAll("bar")
      .data(data)
    .enter().append("rect")
      .attr("class", "bar")
      .attr("x", function(d) { return x(d.Keyword); })
      .attr("width", x.rangeBand())
      .attr("y", function(d) { return y(d.Count); })
      .attr("height", function(d) { return height - y(d.Count); });
});

//////////////////////////////////////////////////////////////////
//
// Hashtag Chart
//
// add the SVG element
var hashtag = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");
// load the data
d3.json("/data/hashtags", function(error, data) {
    data.forEach(function(d) {
        d.Hashtag = d.Hashtag;
        d.Count = +d.Count;
    });
  // scale the range of the data
  x.domain(data.map(function(d) { return d.Hashtag; }));
  y.domain([0, d3.max(data, function(d) { return d.Count; })]);
  // add axis
  hashtag.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis)
    .selectAll("text")
      .style("text-anchor", "end")
      .attr("dx", "-.8em")
      .attr("dy", "-.55em")
      .attr("transform", "rotate(-90)" );
  hashtag.append("g")
      .attr("class", "y axis")
      .call(yAxis)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 5)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Frequency");
  // Add bar chart
  hashtag.selectAll("bar")
      .data(data)
    .enter().append("rect")
      .attr("class", "bar")
      .attr("x", function(d) { return x(d.Hashtag); })
      .attr("width", x.rangeBand())
      .attr("y", function(d) { return y(d.Count); })
      .attr("height", function(d) { return height - y(d.Count); });
});


//////////////////////////////////////////////////////////////////
//
// Counts Chart
//
// add the SVG element
var count = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");

// load the data
d3.json("/data/counts", function(error, data) {
    data.forEach(function(d) {
        d.Time = d.Time;
        d.Count = d.Count;
    });
  // scale the range of the data
  x.domain(data.map(function(d) { return d.Time; }));
  //x.domain([0, d3.max(data, function(d) { return d.Time; })]);
  y.domain([0, d3.max(data, function(d) { return d.Count; })]);
  // add axis
  count.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis)
    .selectAll("text")
      .style("text-anchor", "end")
      .attr("dx", "-.8em")
      .attr("dy", "-.55em")
      .attr("transform", "rotate(-90)" );
  count.append("g")
      .attr("class", "y axis")
      .call(yAxis)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 5)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Count");

    var line = d3.svg.line()
            .x(function(d) { return 30+x(d.Time); })
            .y(function(d) { return y(d.Count);});


  count.selectAll("path")
    .data(data)
    .enter().append("path")
    .attr("class", "line")
    .attr("fill", "none")
    .attr("stroke", "blue")
    .attr("stroke-width", 3)
    .attr("d", line(data));

});

//////////////////////////////////////////////////////////////////
//
// Ratio Chart
//
// add the SVG element
var ratio = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left  + "," + margin.top +1 + ")");

// load the data
//d3.json("/data/ratio", function(error, data) {
//    data.forEach(function(d) {
//        d.Pos = d.Ratio;
	//d.Neg = 1-d.Ratio;
//    });

  var arc = d3.svg.arc()
                .innerRadius(40)
                .outerRadius(60);

  var data = [0.72, 0.28];
  var pie = d3.layout.pie();
  var color = d3.scale.category10();

  arcs = ratio.selectAll("g.arc") 
    .data(pie(data))
    .enter().append("g")
    .attr("class", "arc");

  //Draw arc paths
  arcs.append("path")
    .attr("fill", function(d, i) {
    return color(i);
    })
    .attr("d", arc);

//});



















