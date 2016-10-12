


var time = d3.select("#time").append("svg")
    .attr("height", 120)
    .append("g")
    .attr("transform",
          "translate(" + 10 + "," + 70 + ")");

var time_t = 0
// load the data
d3.json("/data/time", function(error, data) {
    data.forEach(function(d) {
	time_t += d.Time;
    });  

   time.append("text")
        .text(time_t+' s')
        .style("font-size","50px");

});







