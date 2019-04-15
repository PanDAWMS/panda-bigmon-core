function stackedHistOrdinal(data, divToShow, title) {


    var colors = ["#116aff", "#fe8504", "#1ff7fe", "#f701ff", "#2e4a02", "#ffaad5", "#f1ff8d", "#1eff06", "#700111", "#1586c3", "#ff067d", "#0e02fb", "#1bffa1", "#921e8f", "#c49565", "#fd0128", "#4ea105", "#158279", "#c8fe0a", "#fdcc0b", "#834969", "#ff7673", "#05018b", "#c591fe", "#a6d8ab", "#948c01", "#484ba1", "#fe22c0", "#06a05d", "#694002", "#8e39e9", "#bdc6ff", "#030139", "#b33802", "#85fa60", "#a2025b", "#3e021b", "#ffcd6d", "#4a92ff", "#e564b6", "#43cfff", "#7e9051", "#e768fc", "#09406b", "#b17005", "#8fd977", "#c1063e", "#a7594f", "#14e3b8", "#bccb1e", "#53064f", "#fff1b7", "#997dba", "#fe965c", "#ffb0a7", "#046c04", "#8451ce", "#d46585", "#fef70c", "#1003c3", "#024a2e", "#0fc551", "#1f025d", "#fd5302", "#5bbfc4", "#481903", "#bfc066", "#ad04bb", "#efa425", "#06c709", "#9701ff", "#84468e", "#018da8", "#88cf01", "#6d6412", "#658a1d", "#0d3cb4", "#144cfe", "#fe5d43", "#33753e", "#4cb28f", "#e6b4ff", "#a5feef", "#caff68", "#d80f8a", "#79193a", "#97fdba", "#a85726", "#fe8cf9", "#8bfe01", "#4a315d", "#ff0155", "#02ff5e", "#6b0199", "#bc7e9f", "#fde75c"];

    var formatCount = d3.format(",.0f");
    var formatXAxis = d3.format(".2s");
    var formatStats = d3.format(".3s");

    var margin = {top: 30, right: 100, bottom: 100, left: 70},
        width = 650 - margin.left - margin.right,
        height = 400 - margin.top - margin.bottom;

    var values = data['sites'];
    var ranges = data['ranges'];
    var stats = data['stats'];

    var statistics = [{type: "\u03BC", val:formatStats(stats[0])}];
	statistics.push({type:"\u03C3", val:formatStats(stats[1])});

    var lowerBand = ranges[0];
    var upperBand = ranges[ranges.length - 1];


    var x = d3.scale.ordinal()
        .domain(ranges)
        .rangeRoundPoints([0, width]);

    numberofbins = ranges.length - 1;
    var filterBase = 1;

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom")
        .tickValues(ranges.map( function(d,i) {
            if(i % filterBase === 0 ) {
                return d;
              }
            })
            .filter(function (d)
            {
                return !!d;
            }
            ))
        .tickFormat(formatXAxis);
    var y = d3.scale.linear().range([height, 0]);
    var yAxis = d3.svg.axis().scale(y)
        .orient("left");

    var intermediateData = [];
    var layer = 0;
    Object.keys(values).forEach(function(key) {
        intermediateData.push({site:key, values: []});
        values[key].forEach(function (d,i) {
            intermediateData[layer].values.push({x: x(ranges[i]), y:parseInt(d,10)})
        });
        layer +=1;
    });

    var stackData = d3.layout.stack()
        .values(function (d) {
        return d.values;
    } );

    var stackedData = stackData(intermediateData);

    var color = d3.scale.ordinal().range(colors);

    y.domain([0, d3.max(stackedData[stackedData.length - 1].values, function (d) {
        return d.y + d.y0;
    })]);

    if (stackedData.length > 4) {
        margin.bottom += Math.floor(stackedData.length / 4) * 12;
    }

    var svg = d3.select(divToShow)
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
    var bin = svg.selectAll(".site")
        .data(stackedData)
        .enter().append("g")
        .attr("class", "site")
        .style("fill", function (d, i) {
            return color(d.site);
        })
        .style("stroke", function (d, i) {
            return d3.rgb(color(d.site)).darker();
        })
        .style("stroke-width", 0.4);

    bin.selectAll(".bar")
        .data(function (d) {
            return d.values;
        })
        .enter().append("rect")
        .attr("class", "bar")
        .attr("x", function (d) {
            return d.x;
        })
        .attr("width", width / numberofbins - 1)
        .attr("y", function (d) {
            return y(d.y0 + d.y);
        })
        .attr("height", function (d) {
            return y(d.y0) - y(d.y0 + d.y);
        });
    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)
        .selectAll("text")
        .attr("dx", -32)
        .attr("dy", 5)
        .attr("transform", function (d) {
            return "rotate(-45)"
        });
    if (title.indexOf('PSS') >= 0) {
        svg.append("g")
            .attr("transform", "translate(" + (width + 10) + " ," + (height + 15) + ")")
            .append("text")
            .style("text-anchor", "left")
            .text("PSS, MB");
    }
    if (title.indexOf('Walltime') >= 0) {
        svg.append("g")
            .attr("transform", "translate(" + (width + 10) + " ," + (height + 15) + ")")
            .append("text")
            .style("text-anchor", "left")
            .text("Time, s");
    }
    if (title.indexOf('HS06') >= 0) {
        svg.append("g")
            .attr("transform", "translate(" + (width + 10) + " ," + (height + 15) + ")")
            .append("text")
            .style("text-anchor", "left")
            .text("HS06s");
    }
    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis);
    svg.append("g")
        .attr("transform", "rotate(-90)")
        .append("text")
        .attr("y", 0 - margin.left)
        .attr("x", 0 - (height / 2))
        .attr("dy", "1em")
        .style("text-anchor", "middle")
        .text("N jobs");

    svg.append("g")
        .attr("transform", "translate(" + (width / 2) + ", -10)")
        .append("text")
        .attr("class", "title")
        .text(title);

    var squareside = 10;
    var legend = svg.selectAll(".legend")
        .data(color.domain().slice())
        .enter().append("g")
        .attr("class", "legend")
        .attr("transform", function (d, i) {
            maxLegendWidth = (i % 4) * (width + 3 * (margin.left + margin.right) / 4) / 4;
            maxLegendHeight = Math.floor(i / 4) * 12;
            return "translate(" + (maxLegendWidth - 3 * margin.left / 4) + ", " + (height + margin.top + 30 + maxLegendHeight) + ")";
        });

    legend.append("rect")
        .attr("x", 0)
        .attr("width", squareside)
        .attr("height", squareside)
        .style("fill", color)
        .style({"stroke": d3.rgb(color).darker(), 'stroke-width': 0.4});

    legend.append("text")
        .attr("x", squareside + 5)
        .attr("y", 10)
        .text(function (d) {
            return d;
        });

    var statlegend = svg.selectAll(".statlegend")
        .data(statistics)
        .enter()
        .append("g")
        .attr("class", "statlegend")
        .attr("transform", function (d, i) {
            return "translate(" + (width) + ", " + (margin.top + (i - 1) * 15) + ")";
        });
    statlegend.append("text")
        .attr("x", 0)
        .attr("y", 0)
        .attr("class", "stattext")
        .text(function (d) {
            return d.type + "=" + d.val;
        });
}