//////////////////////////////////////////////////////////////////
//
// Ratio Chart
//
var margin = {top: 20, right: 100, bottom: 70, left: 60},
    width = 400 - margin.left - margin.right,
    height = 300 - margin.top - margin.bottom;

// add the SVG element
var ratio = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform",
          "translate(" + width  + "," + height/2 + ")");

var pie = d3.layout.pie()
    .sort(null)
    .value(function(d) { return d.Ratio; });


var arc = d3.svg.arc()
         .innerRadius(0)
         .outerRadius(100);

var color = d3.scale.category10();

// load the data
d3.json("/data/ratio", function(error, data) {
    data.forEach(function(d) {
        d.Pos = d.Ratio;
	d.Neg = 1-d.Ratio;
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


});



















