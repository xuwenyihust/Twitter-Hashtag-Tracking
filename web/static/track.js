//////////////////////////////////////////////////////////////////
//
// 
//
var track = d3.select("#track").append("svg")
              .attr("height", 120)
              .append("g")
              .attr("transform",
                    "translate(" + 40 + "," + 70 + ")");

track.append("text")
     .text("#overwatch")
     .style("font-size","50px");

// load the data
//d3.json("../../conf/parameters.json", function(error, data) {
//    data.forEach(function(d) {
//        d.hashtag = d.hashtag;
//    });

  //track.text(d.hashtag)
//  track.text("#overwatch");

//});



















