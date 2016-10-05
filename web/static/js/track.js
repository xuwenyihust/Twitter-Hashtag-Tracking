//////////////////////////////////////////////////////////////////
//
// 
//
var track = d3.select("#track").append("svg")
              .attr("height", 120)
              .append("g")
              .attr("transform",
                    "translate(" + 10 + "," + 70 + ")");
var tr = ''
// load the data
d3.json("data/tracking_word", function(error, data) {
    data.forEach(function(d) {
        tr = d.Tracking_word;
    });


  track.append("text") 
       .text(tr)
       .style("font-size","30px");
});



















