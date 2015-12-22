/**
 * Created by spadolski on 12/22/15.
 */

function pandamonplotFunc(values, divToShow, title) {


    // A formatter for counts.
    var formatCount = d3.format(",.0f");

    var margin = {top: 10, right: 30, bottom: 30, left: 30},
        width = 500 - margin.left - margin.right,
        height = 300 - margin.top - margin.bottom;

    var lowerBand = Math.min.apply(Math, values);
    var upperBand = Math.max.apply(Math, values);
    var x = d3.scale.linear().domain([lowerBand, upperBand]).range([0, width]);
    var data = d3.layout.histogram()
        .bins(x.ticks(50))
        (values);

    var y = d3.scale.linear()
        .domain([0, d3.max(data, function (d) {
            return d.y;
        })])
        .range([height, 0]);

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");

    var svg = d3.select(divToShow).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var bar = svg.selectAll(".bar")
        .data(data)
        .enter().append("g")
        .attr("class", "bar")
        .attr("transform", function (d) {
            return "translate(" + x(d.x) + "," + y(d.y) + ")";
        });

    bar.append("rect")
        .attr("x", 1)
        .attr("width", x(data[0].dx) - 2)
        .attr("height", function (d) {
            return height - y(d.y);
        });

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

    svg.append("g")
        .attr("class", "y axis")
        .call(d3.svg.axis()
            .scale(y)
            .orient("left"));

    svg.append("g")
        .attr("transform", "translate(" + (width / 2) + ", 15)")
        .append("text")
        .text(title)
        .style({"text-anchor": "middle", "font-family": "Arial", "font-weight": "800"});

}