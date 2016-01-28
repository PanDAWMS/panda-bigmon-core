/**
 * Created by spadolski on 12/22/15.
 */

function pandamonplotFunc(values, clouds, divToShow, title, numberofbins) {

    var colorPool = {US: "#00006B",
                DE: "#000000",
                CERN: "#AE3C51",
                UK: "#356C20",
                FR: "#0055A5",
                IT: "#009246",
                NL: "#D97529",
                CA: "#FF1F1F",
                ND: "#6998FF",
                ES: "#EDBF00",
                RU: "#66008D",
                TW: "#89000F"
        };
    var colors = Object.keys(colorPool).map(function (k) { return colorPool[k]; });
    var colorsName = Object.keys(colorPool).map(function (k) { return k; });

    var formatCount = d3.format(",.0f");

    var margin = {top: 30, right: 70, bottom: 60, left: 30},
        width = 600 - margin.left - margin.right,
        height = 350 - margin.top - margin.bottom;

    var lowerBand = d3.min(values);
    var upperBand = d3.max(values);

    var x = d3.scale.linear()
        .domain([lowerBand, upperBand])
        .range([0, width])
        .nice();
    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");
    var y = d3.scale.linear().range([height, 0]);
    var yAxis = d3.svg.axis().scale(y)
        .orient("left");
    var stack = d3.layout.stack()
        .values(function(d) {
            return d.values;
        });

    if (lowerBand == upperBand) {
        numberofbins=2;
        x.domain([lowerBand-1,upperBand+1]);
        xAxis.ticks(1);
    }

    var color = d3.scale.ordinal()
        .domain(colorsName)
        .range(colors);

    var data = [{'value': 0, 'cloud': ''}];

    for(var i = 0, ii = values.length; i<ii; i++) {
        data[i]={'value': values[i], 'cloud': clouds[i]}
    }

    var svg = d3.select(divToShow)
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var binByCloud = d3.layout.histogram()
        .value(function(d) { return d.value; })
        .bins(x.ticks(numberofbins));

    var dataGroupedByCloud = d3.nest()
        .key(function(d) { return d['cloud']; })
        .map(data, d3.map);

    var histDataByCloud = [];
    dataGroupedByCloud.forEach(function(key, value) {
            // Bin the data for each borough by month
            var histData = binByCloud(value);
            histDataByCloud.push({
                cloud: key,
                values: histData
            });
        });

    var stackedHistData = stack(histDataByCloud);

    y.domain([0, d3.max(stackedHistData[stackedHistData.length - 1].values, function(d) {
            return d.y + d.y0;
        })]);

    var cloud = svg.selectAll(".cloud")
            .data(stackedHistData)
          .enter().append("g")
            .attr("class", "cloud")
            .style("fill", function(d, i) {
                return color(d.cloud);
            })
            .style("stroke", function(d, i) {
                return d3.rgb(color(d.cloud)).darker();
            });

    cloud.selectAll(".bar")
            .data(function(d) {
                return d.values;
            })
          .enter().append("rect")
            .attr("class", "bar")
            .attr("x", function(d) {
                return x(d.x);
            })
            .attr("width", width/ (x.ticks(numberofbins).length))
            .attr("y", function(d) {
                return y(d.y0 + d.y);
            })
            .attr("height", function(d) {
                return y(d.y0) - y(d.y0 + d.y);
            });
    if (lowerBand == upperBand) {
        cloud.selectAll(".bar")
            .attr("x",width/4)
            .attr("width",width/2);
    }
    svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis);

    svg.append("g")
            .attr("class", "y axis")
            .call(yAxis);

    svg.append("g")
            .attr("transform", "translate(" + (width/2) + ", -10)")
            .append("text")
            .attr("class", "title")
            .text(title);

    var maxLegendWidth = 30;
    var maxLegendHeight = (height/12);
    var xStart = 8;
    var squareside = 12;
    var legend = svg.selectAll(".legend")
            .data(color.domain().slice())
          .enter().append("g")
            .attr("class", "legend")
            .attr("transform", function(d, i) {
                return "translate(" + width + ", " + (i * maxLegendHeight + 10) + ")";
            });

    legend.append("rect")
            .attr("x", xStart)
            .attr("width", squareside)
            .attr("height", squareside)
            .style("fill", color)
            .style({"stroke":d3.rgb(color).darker(),'stroke-width':0.5});

    legend.append("text")
            .attr("x", xStart+squareside+5)
            .attr("y", 10)
            .text(function(d) {
                return d;
            });

}