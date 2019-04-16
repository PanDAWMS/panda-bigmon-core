function stackedHistOrdinal(data, divToShow, title) {


    var colors = ["#116aff", "#fe8504", "#1ff7fe", "#f701ff", "#2e4a02", "#ffaad5", "#f1ff8d", "#1eff06", "#700111", "#1586c3", "#ff067d", "#0e02fb", "#1bffa1", "#921e8f", "#c49565", "#fd0128", "#4ea105", "#158279", "#c8fe0a", "#fdcc0b", "#834969", "#ff7673", "#05018b", "#c591fe", "#a6d8ab", "#948c01", "#484ba1", "#fe22c0", "#06a05d", "#694002", "#8e39e9", "#bdc6ff", "#030139", "#b33802", "#85fa60", "#a2025b", "#3e021b", "#ffcd6d", "#4a92ff", "#e564b6", "#43cfff", "#7e9051", "#e768fc", "#09406b", "#b17005", "#8fd977", "#c1063e", "#a7594f", "#14e3b8", "#bccb1e", "#53064f", "#fff1b7", "#997dba", "#fe965c", "#ffb0a7", "#046c04", "#8451ce", "#d46585", "#fef70c", "#1003c3", "#024a2e", "#0fc551", "#1f025d", "#fd5302", "#5bbfc4", "#481903", "#bfc066", "#ad04bb", "#efa425", "#06c709", "#9701ff", "#84468e", "#018da8", "#88cf01", "#6d6412", "#658a1d", "#0d3cb4", "#144cfe", "#fe5d43", "#33753e", "#4cb28f", "#e6b4ff", "#a5feef", "#caff68", "#d80f8a", "#79193a", "#97fdba", "#a85726", "#fe8cf9", "#8bfe01", "#4a315d", "#ff0155", "#02ff5e", "#6b0199", "#bc7e9f", "#fde75c"];

    var formatCount = d3.format(",.0f");
    var formatYAxis = d3.format(".2s");
    var formatStats = d3.format(".3s");

    var margin = {top: 30, right: 100, bottom: 200, left: 70},
        width = 1024 - margin.left - margin.right,
        height = 600 - margin.top - margin.bottom;


    var x_keys = Object.keys(data);

    var fields = [];
    Object.keys(data).forEach(function (key) {
        Object.keys(data[key]).forEach(function (keyi) {
            if (fields.indexOf(keyi) < 0) {
                fields.push(keyi)
            }
        })
    });

    // var statistics = [{type: "\u03BC", val:formatStats(stats[0])}];
	// statistics.push({type:"\u03C3", val:formatStats(stats[1])});


    var x = d3.scale.ordinal()
        .domain(x_keys)
        .rangeRoundBands([0, width], 0.1);

    var numberofbins = x_keys.length - 1;
    var filterBase = 1;

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom")
        .tickValues(x_keys);
    var y = d3.scale.linear().range([height, 0]);
    var yAxis = d3.svg.axis().scale(y)
        .orient("left")
        .tickFormat(formatYAxis);

    var intermediateData = [];
    fields.forEach(function (v) {
        intermediateData.push({field:v, values: []})
    });

    var layer = 0;
    Object.keys(data).forEach(function(key) {
        Object.keys(data[key]).forEach(function (keyi) {
            intermediateData[fields.indexOf(keyi)].values.push({x: x(key), y:parseInt(data[key][keyi],10)})
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
    var bin = svg.selectAll(".field")
        .data(stackedData)
        .enter().append("g")
        .attr("class", "field")
        .style("fill", function (d, i) {
            return color(d.field);
        })
        .style("stroke", function (d, i) {
            return d3.rgb(color(d.field)).darker();
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
        .style("text-anchor", "end")
        .attr("dx", -10)
        .attr("dy", 0)
        .attr("transform", function (d) {
            return "rotate(-65)"
        });

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
        .text("Sum of CPU consumption");

    svg.append("g")
        .attr("transform", "translate(" + (width / 2) + ", -10)")
        .append("text")
        .attr("class", "title")
        .text(title);

    var squareside = 10;
    const yaxisheight = 6*Math.max.apply(Math, x_keys.map(function (el) {return el.length })) + 10;
    var legend = svg.selectAll(".legend")
        .data(color.domain().slice())
        .enter().append("g")
        .attr("class", "legend")
        .attr("transform", function (d, i) {
            maxLegendWidth = (i % 4) * (width + 3 * (margin.left + margin.right) / 4) / 4;
            maxLegendHeight = Math.floor(i / 4) * 12;
            return "translate(" + (maxLegendWidth - 3 * margin.left / 4) + ", " + (height + margin.top + yaxisheight + maxLegendHeight) + ")";
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

    // var statlegend = svg.selectAll(".statlegend")
    //     .data(statistics)
    //     .enter()
    //     .append("g")
    //     .attr("class", "statlegend")
    //     .attr("transform", function (d, i) {
    //         return "translate(" + (width) + ", " + (margin.top + (i - 1) * 15) + ")";
    //     });
    // statlegend.append("text")
    //     .attr("x", 0)
    //     .attr("y", 0)
    //     .attr("class", "stattext")
    //     .text(function (d) {
    //         return d.type + "=" + d.val;
    //     });
}