//////////////////////////////////////////////////////////////////
//
// Ratio Chart
//
var ratio_margin = {top: 20, right: 100, bottom: 30, left: 120},
    ratio_width = 400 - ratio_margin.left - ratio_margin.right,
    ratio_height = 300 - ratio_margin.top - ratio_margin.bottom;

// add the SVG element
var ratio = d3.select("#ratio").append("svg")
    .attr("width", ratio_width + ratio_margin.left + ratio_margin.right)
    .attr("height", ratio_height + ratio_margin.top + ratio_margin.bottom)
    .append("g")
    .attr("transform",
          "translate(" + ratio_width  + "," + ratio_height/2 + ")");

var labelArc = d3.svg.arc()
    .outerRadius(100)
    .innerRadius(40);


var pie = d3.layout.pie()
    .sort(null)
    .value(function(d) { return d.Ratio; });



var arc = d3.svg.arc()
         .innerRadius(50)
         .outerRadius(100);

var color = d3.scale.category10();

// load the data
var pn = 0
d3.json("/data/ratio", function(error, data) {
    data.forEach(function(d) {
        d.PN = d.PN;
    }); 


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

   arcs.append("text")
     .attr("transform", function(d) {	
 		return "translate(" + labelArc.centroid(d) + ")"; })
     .attr("text-anchor", "middle")
     .attr("dy", "0em")
     .text(function(d) { return d.data.PN; });         
      
});


















