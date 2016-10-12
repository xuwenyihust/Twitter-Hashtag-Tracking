//////////////////////////////////////////////////////////////////
//
// 
//
var user = d3.select("#user").append("svg")
    .attr("height", 120)
    .append("g")
    .attr("transform",
          "translate(" + 20 + "," + 70 + ")");

var t = 0
// load the data
d3.json("/data/users", function(error, data) {
    data.forEach(function(d) {
	t += d.Users;
    });  

   user.append("text")
        .text(t)
        .style("font-size","50px");

});







