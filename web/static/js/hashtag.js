//////////////////////////////////////////////////////////////////
//
// Hashtag Chart
//
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
var hashtag = d3.select("#hashtag").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");

// Prep the tooltip bits, initial display is hidden
var tooltip_hashtag = d3.select('body').append("keyword")
  .attr("class", "tooltip")
  .style("opacity", 0);

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
      .attr("transform", "rotate(-30)" );
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
      .attr("height", function(d) { return height - y(d.Count); })
      .attr("fill", function(d) {return "steelblue"})
      .on("mouseover", function(d) {
        d3.select(this)
          .attr("fill", "blue");
        tooltip_hashtag.transition()
          .duration(50)
          .style("opacity", 0);
        tooltip_hashtag.transition()
          .duration(20)
          .style("opacity", .9);
        tooltip_hashtag.html("Count:" + "<br/>" + d.Count )
         .style("left", (d3.event.pageX) + "px")
         .style("top", (d3.event.pageY - 80) + "px");
      })
      .on("mouseout", function() {
        d3.select(this)
          .attr("fill", "steelblue")
        tooltip_hashtag.transition()
          .duration(50)
          .style("opacity", 0);
      });

});














