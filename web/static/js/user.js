//////////////////////////////////////////////////////////////////
//
// 
//
var user = d3.select("#user").append("svg")
    .attr("height", 120)
    .append("g")
    .attr("transform",
          "translate(" + 20 + "," + 70 + ")");

var num_users = 0
// load the data
d3.json("/data/users", function(error, data) {
    data.forEach(function(d) {
	num_users += d.Users;
    });  

   user.append("text")
        .text(num_users)
        .style("font-size","50px");

});







