//////////////////////////////////////////////////////////////////
//
// 
//
var time = d3.select("#time").append("svg")
    .attr("height", 120)
    .append("g")
    .attr("transform",
          "translate(" + 10 + "," + 70 + ")");

time.append("text")
    .text("1 hour")
    .style("font-size","50px");	

//var t = 0
// load the data
//d3.json("/data/counts", function(error, data) {
//    data.forEach(function(d) {
//	t += d.Count;
//    });  

//   total.append("text")
//        .text(t)
//        .style("font-size","50px");

//});







