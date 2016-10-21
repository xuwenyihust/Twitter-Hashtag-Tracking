//////////////////////////////////////////////////////////////////
//
// Counts Chart
//
var count_margin = {top: 20, right: 100, bottom: 70, left: 60},
    count_width = 800 - count_margin.left - count_margin.right,
    count_height = 300 - count_margin.top - count_margin.bottom;

// set the ranges
var count_x = d3.scale.ordinal().rangeRoundBands([0, count_width], .05);
var count_y = d3.scale.linear().range([count_height, 0]);
// define the axis
var count_xAxis = d3.svg.axis()
    .scale(count_x)
    .orient("bottom")
    //.tickSize(-count_height, 0, 0)
    .outerTickSize(0);
    //.ticks(10);

var count_yAxis = d3.svg.axis()
    .scale(count_y)
    .orient("left")
    .tickSize(-count_width, 0, 0)
    .outerTickSize(0)
    .ticks(10);

// Define the div for the tooltip
var tooltip_count = d3.select("body").append("count")	
    .attr("class", "tooltip")				
    .style("opacity", 0);


// add the SVG element
var count = d3.select("#count").append("svg")
    .attr("width", count_width + count_margin.left + count_margin.right)
    .attr("height", count_height + count_margin.top + count_margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + count_margin.left + "," + count_margin.top + ")");


// load the data
d3.json("/data/counts", function(error, data) {
    data.forEach(function(d) {
        d.Time = d.Time;
        d.Count = d.Count;
    });
  // scale the range of the data
  count_x.domain(data.map(function(d) { return d.Time; }));
  //x.domain([0, d3.max(data, function(d) { return d.Time; })]);
  count_y.domain([0, d3.max(data, function(d) { return d.Count; })]);

  // add axis
  // x axis
  count.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + count_height + ")")
      .call(count_xAxis)
      .attr("class", function(d,i) {
          if(i%3 != 0) d3.select(this).remove();
       })
    .selectAll("text")
      .style("text-anchor", "end")
      .attr("dx", "-.0em")
      .attr("dy", "-.1em")
      .attr("class", function(d,i) {
          if(i%3 != 0) d3.select(this).remove();
       })
      .attr("transform", "rotate(-40)" );
  // Remove some x axis
  //var x_tick = d3.selectAll(".g x axis");
  //x_ticks.attr("class", function(d,i) {
  //    if(i%2 != 0) d3.select(this).remove();
  //});

  // y axis
  count.append("g")
      .attr("class", "y axis")
      .call(count_yAxis)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      //.attr("dx", "1.5em")
      .style("text-anchor", "end")
      .text("Count");


    var line = d3.svg.line()
            .x(function(d) { return 9+count_x(d.Time); })
            .y(function(d) { return count_y(d.Count);});


  count.selectAll("path")
    .data(data)
    .enter().append("path")
    .attr("class", "path_line") 
    .attr("d", line(data));

  count.selectAll("dot")
    .data(data)	
    .enter().append("circle")
    .attr("r", 2)
    .attr("cx", function(d) { return 9+count_x(d.Time); })
    .attr("cy", function(d) { return count_y(d.Count); })
    .style("fill", "steelblue")	

    .on("mouseover", function(d) {		
      tooltip_count.transition()		
         .duration(50)		
         .style("opacity", 0);

      tooltip_count.transition()
         .duration(20)
         .style("opacity", .9);		

      tooltip_count.html(d.Count)	
         .style("left", (d3.event.pageX) + "px")		
         .style("top", (d3.event.pageY - 28) + "px");	
     })
					
    .on("mouseout", function(d) {		
      tooltip_count.transition()		
         .duration(50)		
         .style("opacity", 0);	
    });

});



















