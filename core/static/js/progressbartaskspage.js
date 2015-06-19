
<script src="http://d3js.org/d3.v3.min.js"></script>
<script src="/core/static/js/vendor/jquery.js" type="text/javascript"></script>
<script>
function InitChart(chartID, percentage) {
var names = ['progress'],
    progress = [percentage],
    chart,
    width = 100,
    bar_height = 11,
    height = bar_height * names.length;

chart = d3.select($(chartID)[0]) 
  .append('svg')
  .attr('class', 'chart')
  .attr('width', width)
  .attr('height', height);
  
 var x, y;
x = d3.scale.linear()
   .domain([0, 100])
   .range([0, width]);

y = d3.scale.ordinal()
   .domain(progress)
   .rangeBands([0, height]);

chart.selectAll("rect")
   .data(progress)
   .enter().append("rect")
   .attr("x", 0)
   .attr("y", y)
   .attr("width", x)
   .attr("height", y.rangeBand());  
  
  strtitle = [progress + "%"]
chart.selectAll("text")
  .data(strtitle)
  .enter().append("text")
  .attr("x", 45)
  .attr("y", function(d){ return y(d) + y.rangeBand()/2; } )
  .attr("dx", 0)
  .attr("dy", 9)
  .attr("text-anchor", "start")
  .attr("font-family", "Arial")
  .attr("font-size", "11")
  .text(String);
  
 }

 
</script>
