//////////////////////////////////////////////////////////////////
//
// 
//
var total = d3.select("#total").append("svg")
    .attr("height", 120)
    .append("g")
    .attr("transform",
          "translate(" + 20 + "," + 70 + ")");

var t = 0
// load the data
d3.json("/data/counts", function(error, data) {
    data.forEach(function(d) {
	t += d.Count;
    });  

   total.append("text")
        .text(t)
        .style("font-size","50px");

});







