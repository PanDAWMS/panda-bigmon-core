/**
 * Created by spadolski on 12/22/15.
 */

// Replace the confusing G (for Giga) with  the more recognizable B (for Billion) in default SI prefixes.
function hFormat(num) {
    var siFormat = d3.format(",.3s");
return siFormat(num).replace("G", "B");
}

function pandamonplotHist(data, divToShow, title, numberofbins) {


    var colors= ["#116aff", "#fe8504", "#1ff7fe", "#f701ff", "#2e4a02", "#ffaad5", "#f1ff8d", "#1eff06", "#700111", "#1586c3", "#ff067d", "#0e02fb", "#1bffa1", "#921e8f", "#c49565", "#fd0128", "#4ea105", "#158279", "#c8fe0a", "#fdcc0b", "#834969", "#ff7673", "#05018b", "#c591fe", "#a6d8ab", "#948c01", "#484ba1", "#fe22c0", "#06a05d", "#694002", "#8e39e9", "#bdc6ff","#030139",  "#b33802", "#85fa60", "#a2025b", "#3e021b", "#ffcd6d", "#4a92ff", "#e564b6", "#43cfff", "#7e9051", "#e768fc", "#09406b", "#b17005", "#8fd977", "#c1063e", "#a7594f", "#14e3b8", "#bccb1e", "#53064f", "#fff1b7", "#997dba", "#fe965c", "#ffb0a7", "#046c04", "#8451ce", "#d46585", "#fef70c", "#1003c3", "#024a2e", "#0fc551", "#1f025d", "#fd5302", "#5bbfc4", "#481903", "#bfc066", "#ad04bb", "#efa425", "#06c709", "#9701ff", "#84468e", "#018da8", "#88cf01", "#6d6412", "#658a1d", "#0d3cb4", "#144cfe", "#fe5d43", "#33753e", "#4cb28f", "#e6b4ff", "#a5feef", "#caff68", "#d80f8a", "#79193a", "#97fdba", "#a85726", "#fe8cf9", "#8bfe01", "#4a315d", "#ff0155", "#02ff5e", "#6b0199", "#bc7e9f", "#fde75c"];

    var formatCount = d3.format(",.0f");

    var margin = {top: 30, right: 100, bottom: 100, left: 70},
        width = 650 - margin.left - margin.right,
        height = 400 - margin.top - margin.bottom;

    var values = [];
    for (var i = 0, ii = data.length; i<ii; i++) {
        values[i] = data[i].value;
    }
    var lowerBand = d3.min(values);
    var upperBand = d3.max(values);


	var ave = values.reduce(function(a,b){return (a+b);})/values.length;
	var statistics = [{type: "\u03BC", val:ave}];
	if (values.length>1) {statistics.push({type:"\u03C3", val:d3.deviation(values)});}
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

    numberofbins = 3*Math.round((upperBand-lowerBand)*Math.pow(values.length,1/3)/(3.49*d3.deviation(values)));
    if (numberofbins > 100) {
        numberofbins=100;
    }
    if (lowerBand == upperBand) {
        numberofbins=2;
        x.domain([lowerBand-1,upperBand+1]);
        xAxis.ticks(1);
    }

    var binBySite = d3.layout.histogram()
        .value(function(d) { return d.value; })
        .bins(x.ticks(numberofbins));

    var dataGroupedBySite = d3.nest()
        .key(function(d) { return d['site']; })
        .map(data, d3.map);

    var histDataBySite = [];
    dataGroupedBySite.forEach(function(key, value) {
            var histData = binBySite(value);
            histDataBySite.push({
                site: key,
                values: histData
            });
        });

    var stackedHistData = stack(histDataBySite);

    var color = d3.scale.ordinal().range(colors);

    y.domain([0, d3.max(stackedHistData[stackedHistData.length - 1].values, function(d) {
            return d.y + d.y0;
        })]);

    if (stackedHistData.length>4){
        margin.bottom+=Math.floor(stackedHistData.length/4)*12;
    }

    var svg = d3.select(divToShow)
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
    var bin = svg.selectAll(".site")
            .data(stackedHistData)
          .enter().append("g")
            .attr("class", "site")
            .style("fill", function(d, i) {
                return color(d.site);
            })
            .style("stroke", function(d, i) {
                return d3.rgb(color(d.site)).darker();
            })
            .style("stroke-width", 0.4);

    bin.selectAll(".bar")
            .data(function(d) {
                return d.values;
            })
          .enter().append("rect")
            .attr("class", "bar")
            .attr("x", function(d) {
                return x(d.x);
            })
            .attr("width",width/ (x.ticks(numberofbins).length))
            .attr("y", function(d) {
                return y(d.y0 + d.y);
            })
            .attr("height", function(d) {
                return y(d.y0) - y(d.y0 + d.y);
            });
    if (lowerBand == upperBand) {
        bin.selectAll(".bar")
            .attr("x",width/4)
            .attr("width",width/2);
    }
    svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis)
            .selectAll("text")
                .attr("dx", -32)
                .attr("dy", 5)
                .attr("transform", function(d) {
                    return "rotate(-45)"
                });
    if (title.indexOf('PSS')>=0) {
        svg.append("g")
            .attr("transform", "translate(" + (width+10) + " ," + (height + 15) + ")")
            .append("text")
            .style("text-anchor", "left")
            .text("PSS, MB");
    }
    if (title.indexOf('Walltime')>=0) {
        svg.append("g")
            .attr("transform", "translate(" + (width+10) + " ," + (height + 15) + ")")
            .append("text")
            .style("text-anchor", "left")
            .text("Time, s");
    }
    if (title.indexOf('HS06')>=0) {
        svg.append("g")
            .attr("transform", "translate(" + (width+10) + " ," + (height + 15) + ")")
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
        .attr("x",0 - (height / 2))
        .attr("dy", "1em")
        .style("text-anchor", "middle")
        .text("N jobs");

    svg.append("g")
            .attr("transform", "translate(" + (width/2) + ", -10)")
            .append("text")
            .attr("class", "title")
            .text(title);

    if (title.indexOf('Walltime')>=0) {
        svg.append("line")
            .attr("x1", x(ave))
            .attr("y1", height)
            .attr("x2", x(ave))
            .attr("y2", 0)
            .attr("class", "averageline");
    }

    var squareside = 10;
    var legend = svg.selectAll(".legend")
            .data(color.domain().slice())
          .enter().append("g")
            .attr("class", "legend")
            .attr("transform", function(d, i) {
                maxLegendWidth = (i % 4) * (width+3*(margin.left+margin.right)/4)/4;
                maxLegendHeight = Math.floor(i  / 4) * 12;
                return "translate(" + (maxLegendWidth - 3*margin.left/4) + ", " + (height + margin.top + 30 + maxLegendHeight) + ")";
            });

    legend.append("rect")
            .attr("x", 0)
            .attr("width", squareside)
            .attr("height", squareside)
            .style("fill", color)
            .style({"stroke":d3.rgb(color).darker(),'stroke-width':0.4});

    legend.append("text")
            .attr("x", squareside+5)
            .attr("y", 10)
            .text(function(d) {
                return d;
            });

    var statlegend = svg.selectAll(".statlegend")
		.data(statistics)
		.enter()
            .append("g")
            .attr("class", "statlegend")
            .attr("transform", function(d, i) {
                return "translate(" + (width) + ", " + (margin.top + (i-1)*15) + ")";
            });
    statlegend.append("text")
            .attr("x", 0)
            .attr("y", 0)
            .attr("class", "stattext")
            .text(function(d) {
                return d.type + "=" + d.val.toFixed(1);
            });

    if (title.startsWith("Walltime per event histogram") && numberofbins > 50) {
        var adjustbutton = d3.select(divToShow).append("button")
            .attr("class","buttonadjust")
            .attr('style', 'top: ' + (- height - margin.bottom - 10) + 'px; ' +
                           'left: ' + (- width - margin.right - 50 ) + 'px;')
            .attr("float", "left")
            .text("Adjust")
            .on("click", adjust);

    }


    function adjust () {
        var bins = stackedHistData[stackedHistData.length - 1].values;
        var binsh = [];
        for (var i = 0, len = bins.length; i < len; i++) {
            binsh[i] = bins[i].y0 + bins[i].y;
        }

        var q10 = d3.quantile(binsh.sort(d3.ascending), 0.01);
        var q90 = d3.quantile(binsh.sort(d3.ascending), 0.99);

        var rl = (q10 > 0) ? (q10 - 2 * (q90 - q10)) : 0;
        var rh = (q90 + 2 * (q90 - q10) > d3.max(binsh)) ? d3.max(binsh) - 1 : q90 + 2 * (q90 - q10);
        var firstbinx = 0,
            lastbinx = 0;
        for (var i = 0, len = bins.length; i < len; i++) {
            if ((bins[i].y0 + bins[i].y) > rh) {
                firstbinx = bins[i].x;
                break;
            }
        }
        for (var i = 0, len = bins.length; i < len; i++) {
            lastbinx = ((bins[i].y0 + bins[i].y) > rh) ? (bins[i].x + bins[i].dx) : lastbinx;
        }
        var minx = (firstbinx ) > 0 ? Math.floor(firstbinx) : 0,
            maxx = Math.ceil(lastbinx);
        var underlier = 0, outlier = 0;
        for (var i = 0, len = bins.length; i < len; i++) {
            if ((bins[i].x + bins[i].dx) < minx) {
                underlier += (bins[i].y0 + bins[i].y);
            }
            if ((bins[i].x) > maxx) {
                outlier += (bins[i].y0 + bins[i].y);
            }
        }
        var ymax = d3.max(stackedHistData[stackedHistData.length - 1].values, function (d) {
            return d.y + d.y0;
        });
        if (underlier < 0.1 * ymax && outlier < 0.1 * ymax) {
            redraw();
        }
        else {

            q10 = d3.quantile(binsh.sort(d3.ascending), 0.1);
            q90 = d3.quantile(binsh.sort(d3.ascending), 0.9);

            rl = (q10 > 0) ? (q10 - 1.5 * (q90 - q10)) : 0;
            rh = (q90 + 1.5* (q90 - q10) > d3.max(binsh)) ? d3.max(binsh) - 1 : q90 + 1.5 * (q90 - q10);
            firstbinx = 0;
            lastbinx = 0;
            for (var i = 0, len = bins.length; i < len; i++) {
                if ((bins[i].y0 + bins[i].y) > rh) {
                    firstbinx = bins[i].x;
                    break;
                }
            }
            for (var i = 0, len = bins.length; i < len; i++) {
                lastbinx = ((bins[i].y0 + bins[i].y) > rh) ? (bins[i].x + bins[i].dx) : lastbinx;
            }
            minx = (firstbinx - (lastbinx - firstbinx) / 4) > 0 ? (firstbinx - (lastbinx - firstbinx) / 4) : 0;
            maxx = lastbinx + (lastbinx - firstbinx) / 4;
            // sum of underlier and outlier
            underlier = 0;
            outlier = 0;
            for (var i = 0, len = bins.length; i < len; i++) {
                if ((bins[i].x + bins[i].dx) < minx) {
                    underlier += (bins[i].y0 + bins[i].y);
                }
                if ((bins[i].x) > maxx) {
                    outlier += (bins[i].y0 + bins[i].y);
                }
            }
            redraw();
        }


        function redraw() {


            var stat = [];
            stat.push({type: "underlier", val: underlier});
            stat.push({type: "outlier", val: outlier});


            // rebinning most interesting part of histogram
            x.domain([minx, maxx]);
            binBySite.bins(x.ticks(numberofbins));


            histDataBySite = [];
            dataGroupedBySite.forEach(function (key, value) {
                // Bin the data for each borough by month
                var histData = binBySite(value);
                histDataBySite.push({
                    site: key,
                    values: histData
                });
            });
            stackedHistData = stack(histDataBySite);

            y.domain([0, d3.max(stackedHistData[stackedHistData.length - 1].values, function (d) {
                return d.y + d.y0;
            })]);


            var svg = d3.select(divToShow);
            svg.selectAll(".site")
                .data(stackedHistData);

            bin.selectAll(".bar")
                .data(function (d) {
                    return d.values;
                });

            svg.selectAll(".bar")
                .attr("x", function (d) {
                    return x(d.x);
                })
                .attr("width", width / (x.ticks(numberofbins).length))
                .attr("y", function (d) {
                    return y(d.y0 + d.y);
                })
                .attr("height", function (d) {
                    return y(d.y0) - y(d.y0 + d.y);
                });

            svg.select('.x.axis')
                .call(xAxis)
                .selectAll("text")
                .attr("dx", -32)
                .attr("dy", 5)
                .attr("transform", function (d) {
                    return "rotate(-45)"
                });
            svg.select('.y.axis')
                .call(yAxis);

            svg.select(".averageline")
                .attr("x1", x(ave))
                .attr("y1", height)
                .attr("x2", x(ave))
                .attr("y2", 0);

            statlegend.data(stat);
            statlegend.append("text")
                .attr("x", 0)
                .attr("y", 30)
                .attr("class", "stattext")
                .text(function (d) {
                    return d.type + "=" + d.val.toFixed(1);
                });

            d3.select(".buttonadjust")
                .style('opacity', 0);
            svg.select(".title")
                .text(title + ' [adjusted]')
        }

    }
}

function pandamonplotHistNew(data, divToShow, title, numberofbins) {


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
    if (numberofbins > 0 && numberofbins <= 15) { filterBase = 1;}
    else if (numberofbins > 15 && numberofbins <= 30) {filterBase = 2;}
    else if (numberofbins > 30 && numberofbins <= 60) {filterBase = 4;}
    else if  (numberofbins > 60 && numberofbins <=100) {filterBase = 6;}
    else if  (numberofbins > 100) {filterBase = 8;}

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
    //
    // if (title.indexOf('Walltime') >= 0) {
    //     svg.append("line")
    //         .attr("x1", x(ave))
    //         .attr("y1", height)
    //         .attr("x2", x(ave))
    //         .attr("y2", 0)
    //         .attr("class", "averageline");
    // }
    //
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

    // if (title.startsWith("Walltime per event histogram") && numberofbins > 50) {
    //     var adjustbutton = d3.select(divToShow).append("button")
    //         .attr("class", "buttonadjust")
    //         .attr('style', 'top: ' + (-height - margin.bottom - 10) + 'px; ' +
    //             'left: ' + (-width - margin.right - 50 ) + 'px;')
    //         .attr("float", "left")
    //         .text("Adjust")
    //         .on("click", adjust);
    //
    // }
}



function pandamonplotOverlayFunc(values, sites, divToShow, title, numberofbins) {

    var colors= ["#116aff", "#fe8504", "#1ff7fe", "#f701ff", "#2e4a02", "#ffaad5", "#f1ff8d", "#1eff06", "#700111", "#1586c3", "#ff067d", "#0e02fb", "#1bffa1", "#921e8f", "#c49565", "#fd0128", "#4ea105", "#158279", "#c8fe0a", "#fdcc0b", "#834969", "#ff7673", "#05018b", "#c591fe", "#a6d8ab", "#948c01", "#484ba1", "#fe22c0", "#06a05d", "#694002", "#8e39e9", "#bdc6ff","#030139",  "#b33802", "#85fa60", "#a2025b", "#3e021b", "#ffcd6d", "#4a92ff", "#e564b6", "#43cfff", "#7e9051", "#e768fc", "#09406b", "#b17005", "#8fd977", "#c1063e", "#a7594f", "#14e3b8", "#bccb1e", "#53064f", "#fff1b7", "#997dba", "#fe965c", "#ffb0a7", "#046c04", "#8451ce", "#d46585", "#fef70c", "#1003c3", "#024a2e", "#0fc551", "#1f025d", "#fd5302", "#5bbfc4", "#481903", "#bfc066", "#ad04bb", "#efa425", "#06c709", "#9701ff", "#84468e", "#018da8", "#88cf01", "#6d6412", "#658a1d", "#0d3cb4", "#144cfe", "#fe5d43", "#33753e", "#4cb28f", "#e6b4ff", "#a5feef", "#caff68", "#d80f8a", "#79193a", "#97fdba", "#a85726", "#fe8cf9", "#8bfe01", "#4a315d", "#ff0155", "#02ff5e", "#6b0199", "#bc7e9f", "#fde75c"];

    var formatCount = d3.format(",.0f");

    var margin = {top: 30, right: 100, bottom: 100, left: 70},
        width = 650 - margin.left - margin.right,
        height = 400 - margin.top - margin.bottom;

    var lowerBand = d3.min(values);
    var upperBand = d3.max(values);

	var ave = values.reduce(function(a,b){return (a+b);})/values.length;
	var statistics = [{type: "\u03BC", val:ave}];
	if (values.length>1) {statistics.push({type:"\u03C3", val:d3.deviation(values)});}
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
    numberofbins = 3*Math.round((upperBand-lowerBand)*Math.pow(values.length,1/3)/(3.49*d3.deviation(values)));
    if (lowerBand == upperBand) {
        numberofbins=2;
        x.domain([lowerBand-1,upperBand+1]);
        xAxis.ticks(1);
    }

    var data = [{'value': 0, 'site': ''}];

    for(var i = 0, ii = values.length; i<ii; i++) {
        data[i]={'value': values[i], 'site': sites[i]}
    }

    var binBySite = d3.layout.histogram()
        .value(function(d) { return d.value; })
        .bins(x.ticks(numberofbins));

    var dataGroupedBySite = d3.nest()
        .key(function(d) { return d['site']; })
        .map(data, d3.map);

    var histDataBySite = [];
    dataGroupedBySite.forEach(function(key, value) {
            // Bin the data for each borough by month
            var histData = binBySite(value);
            histDataBySite.push({
                site: key,
                values: histData
            });
        });

    var stackedHistData = stack(histDataBySite);

    var bins = stackedHistData[stackedHistData.length - 1].values;
    var binsh = [];
    for (var i = 0, len = bins.length; i < len; i++) {
        binsh[i] = bins[i].y0 + bins[i].y;
    }

    var q10 = d3.quantile(binsh.sort(d3.ascending), 0.1 );
    var q90 = d3.quantile(binsh.sort(d3.ascending), 0.9 );

    var rl = (q10>0) ? (q10-1.5*(q90-q10)) : 0;
    var rh = (q90+1.5*(q90-q10)>d3.max(binsh))? d3.max(binsh)-1 : q90+1.5*(q90-q10);
    var firstbinx=0,
        lastbinx=0;
    for (var i = 0, len = bins.length; i < len; i++) {
        if  ((bins[i].y0 + bins[i].y)>rh) {
            firstbinx = bins[i].x;
            break;
        }
    }
    for (var i = 0, len = bins.length; i < len; i++) {
        lastbinx =  ((bins[i].y0 + bins[i].y)>rh) ? (bins[i].x+bins[i].dx) : lastbinx;
    }
    var minx = (firstbinx-(lastbinx-firstbinx)/4)>0 ? Math.floor(firstbinx-(lastbinx-firstbinx)/4) : 0,
    maxx = Math.ceil(lastbinx+(lastbinx-firstbinx)/4);
    // sum of underlier and outlier
    var underlier=0, outlier=0;
    for (var i = 0, len = bins.length; i < len; i++) {
        if ((bins[i].x+bins[i].dx) < minx) {
            underlier += (bins[i].y0 + bins[i].y);
        }
        if ((bins[i].x) > maxx) {
            outlier += (bins[i].y0 + bins[i].y);
        }
    }
    statistics.push({type:"un", val:underlier});
    statistics.push({type:"ov", val:outlier});

    // rebinning most interesting part of histogram
    x.domain([minx,maxx]);
    binBySite = d3.layout.histogram()
        .value(function(d) { return d.value; })
        .bins(x.ticks(numberofbins));

    dataGroupedBySite = d3.nest()
        .key(function(d) { return d['site']; })
        .map(data, d3.map);

    histDataBySite = [];
    dataGroupedBySite.forEach(function(key, value) {
            // Bin the data for each borough by month
            var histData = binBySite(value);
            histDataBySite.push({
                site: key,
                values: histData
            });
        });
    stackedHistData = stack(histDataBySite);



    var color = d3.scale.ordinal().range(colors);

    y.domain([0, d3.max(stackedHistData[stackedHistData.length - 1].values, function(d) {
            return d.y + d.y0;
        })]);

    if (stackedHistData.length>4){
        margin.bottom+=Math.floor(stackedHistData.length/4)*12;
    }

    var svg = d3.select(divToShow)
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
    var bin = svg.selectAll(".site")
            .data(stackedHistData)
          .enter().append("g")
            .attr("class", "site")
            .style("fill", function(d, i) {
                return color(d.site);
            })
            .style("stroke", function(d, i) {
                return d3.rgb(color(d.site)).darker();
            })
            .style("stroke-width", 0.4);

    bin.selectAll(".bar")
            .data(function(d) {
                return d.values;
            })
          .enter().append("rect")
            .attr("class", "bar")
            .attr("x", function(d) {
                return x(d.x);
            })
            .attr("width",width/ (x.ticks(numberofbins).length))
            .attr("y", function(d) {
                return y(d.y0 + d.y);
            })
            .attr("height", function(d) {
                return y(d.y0) - y(d.y0 + d.y);
            });
    if (lowerBand == upperBand) {
        bin.selectAll(".bar")
            .attr("x",width/4)
            .attr("width",width/2);
    }
    svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis)
            .selectAll("text")
                .attr("dx", -32)
                .attr("dy", 5)
                .attr("transform", function(d) {
                    return "rotate(-45)"
                });
    if (title.indexOf('PSS')>=0) {
        svg.append("g")
            .attr("transform", "translate(" + (width+10) + " ," + (height + 15) + ")")
            .append("text")
            .style("text-anchor", "left")
            .text("PSS, MB");
    }
    if (title.indexOf('Walltime')>=0) {
        svg.append("g")
            .attr("transform", "translate(" + (width+10) + " ," + (height + 15) + ")")
            .append("text")
            .style("text-anchor", "left")
            .text("Time, s");
    }
    if (title.indexOf('HS06')>=0) {
        svg.append("g")
            .attr("transform", "translate(" + (width+10) + " ," + (height + 15) + ")")
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
        .attr("x",0 - (height / 2))
        .attr("dy", "1em")
        .style("text-anchor", "middle")
        .text("N jobs");

    svg.append("g")
            .attr("transform", "translate(" + (width/2) + ", -10)")
            .append("text")
            .attr("class", "title")
            .text(title + ' (adjusted)');

    if (title.indexOf('Walltime')>=0) {
        svg.append("line")
            .attr("x1", x(ave))
            .attr("y1", height)
            .attr("x2", x(ave))
            .attr("y2", 0)
            .attr("class", "averageline");
    }

    var squareside = 10;
    var legend = svg.selectAll(".legend")
            .data(color.domain().slice())
          .enter().append("g")
            .attr("class", "legend")
            .attr("transform", function(d, i) {
                maxLegendWidth = (i % 4) * (width+3*(margin.left+margin.right)/4)/4;
                maxLegendHeight = Math.floor(i  / 4) * 12;
                return "translate(" + (maxLegendWidth - 3*margin.left/4) + ", " + (height + margin.top + 30 + maxLegendHeight) + ")";
            });

    legend.append("rect")
            .attr("x", 0)
            .attr("width", squareside)
            .attr("height", squareside)
            .style("fill", color)
            .style({"stroke":d3.rgb(color).darker(),'stroke-width':0.4});

    legend.append("text")
            .attr("x", squareside+5)
            .attr("y", 10)
            .text(function(d) {
                return d;
            });

    var statlegend = svg.selectAll(".statlegend")
		.data(statistics)
		.enter()
            .append("g")
            .attr("class", "statlegend")
            .attr("transform", function(d, i) {
                return "translate(" + (width) + ", " + (margin.top + (i-1)*15) + ")";
            });
    statlegend.append("text")
            .attr("x", 0)
            .attr("y", 0)
            .text(function(d) {
                return d.type + "=" + d.val.toFixed(1);
            });

}

function pandamonProdRunTaskSumPlotFunc(values,divToShow,title,numberofbins,productiontype){

    var formatCount = d3.format(",.0f");
    var margin = {top: 30, right: 30, bottom: 50, left: 70},
        width = 550 - margin.left - margin.right,
        height = 300 - margin.top - margin.bottom;

    var color = d3.scale.threshold();
    if (productiontype == 'DPD') {
        color.range(["#ffffff", "#248F24", "#ffff00", "#ff7f0e", "#d62728"]);
        color.domain([0, 1.01, 2.01, 3.01]);
        }
    else {
        color.range(["#116aff", "#116aff"]);
        color.domain([0]);
    }
	var axislabels = {'x' : 'Time, days', 'y': 'N tasks'};
    if ( productiontype == 'ES') {
		axislabels.x = 'Number of events';
		axislabels.y = 'N jobs';
	}
    var lowerBand = d3.min(values);
    var upperBand = d3.max(values);

    var x = d3.scale.linear()
        .domain([lowerBand, upperBand])
        .range([0, width])
        .nice();

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");

    if (lowerBand == upperBand) {
        numberofbins=2;
        x.domain([lowerBand-1,upperBand+1]);
        xAxis.ticks(1);
    }

    var data = d3.layout.histogram()
        .bins(x.ticks(numberofbins))
        (values);


    var y = d3.scale.linear()
        .domain([0, d3.max(data, function(d) {return d.y;})])
        .range([height, 0]);
    var yAxis = d3.svg.axis().scale(y)
        .orient("left");



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
        .attr("width", width/ (x.ticks(numberofbins).length)-1)
        .attr("height", function (d) {
            return height - y(d.y);
        })
        .attr("fill", function(d) { return color(d.x+d.dx); });

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);
	svg.append("g")
    	.attr("transform", "translate(" + (width/2) + " ," + (height + margin.bottom-5) + ")")
        .append("text")
        .attr("class", "axislabel")
        .text(axislabels.x);

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis);
	svg.append("g")
        .attr("transform", "rotate(-90)")
		.append("text")
        .attr("y", 0 - margin.left)
        .attr("x",0 - (height / 2))
        .attr("dy", "1em")
        .attr("class", "axislabel")
        .text(axislabels.y);

    svg.append("g")
        .attr("transform", "translate(" + (width / 2) + ", -10)")
        .append("text")
        .attr("class", "title")
        .text(title);

}
function createGauge(divToShow, label) {
    var config =
			{
				size: 180,
				label: label,
				minorTicks: 5
			};

			config.greenZones = [{ from: 120000, to: 160000 }];
			config.yellowZones = [{ from: 80000, to: 120000 }];
			config.redZones = [{ from:  40000, to: 80000 }];

			gauges[divToShow] = new Gauge(divToShow, config);
			gauges[divToShow].render();
}

function Gauge(placeholderName, configuration){
	this.placeholderName = placeholderName;

	var self = this; // for internal d3 functions

	this.configure = function(configuration)
	{
		this.config = configuration;

		this.config.size = this.config.size * 0.95;

		this.config.raduis = this.config.size * 0.9 / 2;
		this.config.cx = this.config.size / 2;
		this.config.cy = this.config.size / 2;

		this.config.min = 0;
		this.config.max = 160000;
		this.config.range = this.config.max - this.config.min;

		this.config.majorTicks = configuration.majorTicks || 5;
		this.config.minorTicks = configuration.minorTicks || 2;

		this.config.greenColor 	= configuration.greenColor || "#109618";
		this.config.yellowColor = configuration.yellowColor || "#FF9900";
		this.config.redColor 	= configuration.redColor || "#DC3912";

		this.config.transitionDuration = configuration.transitionDuration || 2000;
	};

	this.render = function()
	{
		this.body = d3.select("#" + this.placeholderName)
							.append("svg:svg")
							.attr("class", "gauge")
							.attr("width", this.config.size)
							.attr("height", this.config.size)
                            .attr("transform", "translate(" + 0 + "," + 0 + ")");

		this.body.append("svg:circle")
					.attr("cx", this.config.cx)
					.attr("cy", this.config.cy)
					.attr("r", this.config.raduis)
					.style("fill", "#ccc")
					.style("stroke", "#000")
					.style("stroke-width", "0.5px");

		this.body.append("svg:circle")
					.attr("cx", this.config.cx)
					.attr("cy", this.config.cy)
					.attr("r", 0.9 * this.config.raduis)
					.style("fill", "#fff")
					.style("stroke", "#e0e0e0")
					.style("stroke-width", "2px");

		for (var index in this.config.greenZones)
		{
			this.drawBand(this.config.greenZones[index].from, this.config.greenZones[index].to, self.config.greenColor);
		}

		for (var index in this.config.yellowZones)
		{
			this.drawBand(this.config.yellowZones[index].from, this.config.yellowZones[index].to, self.config.yellowColor);
		}

		for (var index in this.config.redZones)
		{
			this.drawBand(this.config.redZones[index].from, this.config.redZones[index].to, self.config.redColor);
		}

		if (undefined != this.config.label)
		{
			var fontSize = Math.round(this.config.size / 12);
			this.body.append("svg:text")
						.attr("x", this.config.cx)
						.attr("y", this.config.cy / 2 + fontSize / 2)
						.attr("dy", fontSize / 2)
						.attr("text-anchor", "middle")
						.text(this.config.label)
						.style("font-size", fontSize + "px")
						.style("fill", "#333")
						.style("stroke-width", "0px");
		}

		var fontSize = Math.round(this.config.size / 16);
		var majorDelta = this.config.range / (this.config.majorTicks - 1);
		for (var major = this.config.min; major <= this.config.max; major += majorDelta)
		{
			var minorDelta = majorDelta / this.config.minorTicks;
			for (var minor = major + minorDelta; minor < Math.min(major + majorDelta, this.config.max); minor += minorDelta)
			{
				var point1 = this.valueToPoint(minor, 0.75);
				var point2 = this.valueToPoint(minor, 0.85);

				this.body.append("svg:line")
							.attr("x1", point1.x)
							.attr("y1", point1.y)
							.attr("x2", point2.x)
							.attr("y2", point2.y)
							.style("stroke", "#666")
							.style("stroke-width", "1px");
			}

			var point1 = this.valueToPoint(major, 0.7);
			var point2 = this.valueToPoint(major, 0.85);

			this.body.append("svg:line")
						.attr("x1", point1.x)
						.attr("y1", point1.y)
						.attr("x2", point2.x)
						.attr("y2", point2.y)
						.style("stroke", "#333")
						.style("stroke-width", "2px");

			if (major == this.config.min || major == this.config.max)
			{
				var point = this.valueToPoint(major, 0.63);

				this.body.append("svg:text")
				 			.attr("x", point.x)
				 			.attr("y", point.y)
				 			.attr("dy", fontSize / 3)
				 			.attr("text-anchor", major == this.config.min ? "start" : "end")
				 			.text(major)
				 			.style("font-size", fontSize + "px")
							.style("fill", "#333")
							.style("stroke-width", "0px");
			}
		}

		var pointerContainer = this.body.append("svg:g").attr("class", "pointerContainer");

		var midValue = (this.config.min + this.config.max) / 2;

		var pointerPath = this.buildPointerPath(midValue);

		var pointerLine = d3.svg.line()
									.x(function(d) { return d.x })
									.y(function(d) { return d.y })
									.interpolate("basis");

		pointerContainer.selectAll("path")
							.data([pointerPath])
							.enter()
								.append("svg:path")
									.attr("d", pointerLine)
									.style("fill", "#dc3912")
									.style("stroke", "#c63310")
									.style("fill-opacity", 0.7)

		pointerContainer.append("svg:circle")
							.attr("cx", this.config.cx)
							.attr("cy", this.config.cy)
							.attr("r", 0.12 * this.config.raduis)
							.style("fill", "#4684EE")
							.style("stroke", "#666")
							.style("opacity", 1);

		var fontSize = Math.round(this.config.size / 12);
		pointerContainer.selectAll("text")
							.data([midValue])
							.enter()
								.append("svg:text")
									.attr("x", this.config.cx)
									.attr("y", this.config.size - this.config.cy / 4 - fontSize)
									.attr("dy", fontSize / 2)
									.attr("text-anchor", "middle")
									.style("font-size", fontSize + "px")
									.style("fill", "#000")
									.style("stroke-width", "0px");

		this.redraw(this.config.min, 0);
	};

	this.buildPointerPath = function(value)
	{
		var delta = this.config.range / 13;

		var head = valueToPoint(value, 0.85);
		var head1 = valueToPoint(value - delta, 0.12);
		var head2 = valueToPoint(value + delta, 0.12);

		var tailValue = value - (this.config.range * (1/(270/360)) / 2);
		var tail = valueToPoint(tailValue, 0.28);
		var tail1 = valueToPoint(tailValue - delta, 0.12);
		var tail2 = valueToPoint(tailValue + delta, 0.12);

		return [head, head1, tail2, tail, tail1, head2, head];

		function valueToPoint(value, factor)
		{
			var point = self.valueToPoint(value, factor);
			point.x -= self.config.cx;
			point.y -= self.config.cy;
			return point;
		}
	};

	this.drawBand = function(start, end, color)
	{
		if (0 >= end - start) return;

		this.body.append("svg:path")
					.style("fill", color)
					.attr("d", d3.svg.arc()
						.startAngle(this.valueToRadians(start))
						.endAngle(this.valueToRadians(end))
						.innerRadius(0.65 * this.config.raduis)
						.outerRadius(0.85 * this.config.raduis))
					.attr("transform", function() { return "translate(" + self.config.cx + ", " + self.config.cy + ") rotate(270)" });
	};

	this.redraw = function(value, transitionDuration)
	{
		var pointerContainer = this.body.select(".pointerContainer");

		pointerContainer.selectAll("text").text(Math.round(value));

		var pointer = pointerContainer.selectAll("path");
		pointer.transition()
					.duration(undefined != transitionDuration ? transitionDuration : this.config.transitionDuration)
					//.delay(0)
					//.ease("linear")
					//.attr("transform", function(d)
					.attrTween("transform", function()
					{
						var pointerValue = value;
						if (value > self.config.max) pointerValue = self.config.max + 0.02*self.config.range;
						else if (value < self.config.min) pointerValue = self.config.min - 0.02*self.config.range;
						var targetRotation = (self.valueToDegrees(pointerValue) - 90);
						var currentRotation = self._currentRotation || targetRotation;
						self._currentRotation = targetRotation;

						return function(step)
						{
							var rotation = currentRotation + (targetRotation-currentRotation)*step;
							return "translate(" + self.config.cx + ", " + self.config.cy + ") rotate(" + rotation + ")";
						}
					});
	};

	this.valueToDegrees = function(value)
	{
		// thanks @closealert
		//return value / this.config.range * 270 - 45;
		return value / this.config.range * 270 - (this.config.min / this.config.range * 270 + 45);
	};

	this.valueToRadians = function(value)
	{
		return this.valueToDegrees(value) * Math.PI / 180;
	};

	this.valueToPoint = function(value, factor)
	{
		return { 	x: this.config.cx - this.config.raduis * factor * Math.cos(this.valueToRadians(value)),
					y: this.config.cy - this.config.raduis * factor * Math.sin(this.valueToRadians(value)) 		};
	};

	// initialization
	this.configure(configuration);
}

function pandamonPieChartFunc(values,divToShow,title){

var data = $.map(values, function(value, key) { if (value>0) {return (value*1.0/1000000)} });
var labels = $.map(values, function(value, key) { if (value>0) {return key} });
var neventstot = 0;
for (var i = 0; i < data.length; i++) { neventstot += data[i] << 0;}
var w = 150,
    h = 150,
    r = Math.min(w, h) / 2,
    labelr = r + 10,
    color = d3.scale.ordinal().range(["#ff7f0e", "#2ca02c", "#1f77b4", "#9467bd"]).domain(['evgen' , 'pile', 'simul', 'recon']),
    donut = d3.layout.pie(),
    arc = d3.svg.arc().innerRadius(r * .6).outerRadius(r);

var vis = d3.select(divToShow)
  .append("svg:svg")
    .data([data])
    .attr("width", w + 100)
    .attr("height", h + 100);

var arcs = vis.selectAll("g.arc")
    .data(donut.value(function(d) { return d}))
  .enter().append("svg:g")
    .attr("class", "arc")
    .attr("transform", "translate(" + (r + 50) + "," + (r + 50) + ")");

arcs.append("svg:path")
    .attr("fill", function(d, i) { return color(labels[i]); })
    .attr("d", arc);

arcs.append("text")
    .attr("transform", function(d) {
        var c = arc.centroid(d),
            x = c[0],
            y = c[1],
            // pythagorean theorem for hypotenuse
            h = Math.sqrt(x*x + y*y);
        return "translate(" + (x/h * labelr) +  ',' +
           (y/h * labelr) +  ")";
    })
    .attr("dy", ".35em")
    .attr("text-anchor", function(d) {
        // are we past the center?
        return (d.endAngle + d.startAngle)/2 > Math.PI ?
            "end" : "start";
    })
    .text(function(d, i) { return labels[i]; });

vis.append("g")
        .attr("transform", "translate(" + (w / 2 +  50) + "," + (w / 2 +  40) + ")")
        .append("text")
        .attr("class", "title")
        .text(title);
vis.append("g")
        .attr("transform", "translate(" + (w / 2 +  50) + "," + (w / 2 +  60) + ")")
        .append("text")
        .attr("class", "legendpie")
        .text(neventstot.toFixed(1)+'M events ');
}

function runningProdTasksPieChartFunc(values,divToShow,title){

var formatDecimal = d3.format(",.2f"),
	formatPercent = d3.format(",.1%");
var data = $.map(values, function(value, key) { if (value>0) {return value} });
var labels = $.map(values, function(value, key) { if (value>0) {return key} });
var tot = 0;
for (var i = 0; i < data.length; i++) { tot += data[i];}
var margin = {top: 0, right: 5, bottom: 90, left: 5},
    w = 200 - margin.left - margin.right,
    h = 290 - margin.top - margin.bottom,
    r = Math.min(w, h) / 2;

if (divToShow.indexOf('ProcessingType') > -1 || title.indexOf('FS') > -1 || title.indexOf('AFII') > -1) {
    var color = d3.scale.ordinal()
        .range(["#ff7f0e", "#d62728", "#1f77b4", "#2ca02c", "#9467bd", "#bcbd22", "#17becf", "#e377c2"])
        .domain(['evgen', 'pile', 'simul', 'recon', 'reprocessing', 'deriv', 'merge', 'eventIndex']);
}
else if (divToShow.indexOf('ByStatus') > -1) {
    var color = d3.scale.ordinal()
        .range([ "#248F24", "#47D147",  "#c7c7c7"])
        .domain(['done', 'running', 'waiting']);
}
else if (divToShow.indexOf('TaskStatus') > -1) {
    var color = d3.scale.ordinal()
        .range(["#47D147", "#099999", "#ffbb78", "#7f7f7f", "#FFD65D", "#A0D670", "#DCDCDC", "#c7c7c7", "#98df8a", "#FF8174", "#404040", "#DFDFDF"])
        .domain(['running', 'assigning', 'exhausted', 'paused', 'throttled', 'pending', 'ready', 'registered', 'scouting', 'broken', "passed", 'defined']);
}
else if (divToShow.indexOf('Priority') > -1) {
    color = d3.scale.ordinal()
        .range(["#116aff", "#fe8504", "#1ff7fe", "#f701ff", "#2e4a02", "#ffaad5", "#f1ff8d", "#1eff06", "#700111", "#1586c3", "#ff067d", "#0e02fb", "#1bffa1", "#921e8f", "#c49565", "#fd0128", "#4ea105", "#158279", "#c8fe0a", "#fdcc0b", "#834969", "#ff7673", "#05018b", "#c591fe", "#a6d8ab", "#948c01", "#484ba1", "#fe22c0", "#06a05d", "#694002", "#8e39e9", "#bdc6ff","#030139",  "#b33802", "#85fa60", "#a2025b", "#3e021b", "#ffcd6d", "#4a92ff", "#e564b6", "#43cfff", "#7e9051", "#e768fc", "#09406b", "#b17005", "#8fd977", "#c1063e", "#a7594f", "#14e3b8", "#bccb1e", "#53064f", "#fff1b7", "#997dba", "#fe965c", "#ffb0a7", "#046c04", "#8451ce", "#d46585", "#fef70c", "#1003c3", "#024a2e", "#0fc551", "#1f025d", "#fd5302", "#5bbfc4", "#481903", "#bfc066", "#ad04bb", "#efa425", "#06c709", "#9701ff", "#84468e", "#018da8", "#88cf01", "#6d6412", "#658a1d", "#0d3cb4", "#144cfe", "#fe5d43", "#33753e", "#4cb28f", "#e6b4ff", "#a5feef", "#caff68", "#d80f8a", "#79193a", "#97fdba", "#a85726", "#fe8cf9", "#8bfe01", "#4a315d", "#ff0155", "#02ff5e", "#6b0199", "#bc7e9f", "#fde75c"])
		.domain(labels);
}
else if (divToShow.indexOf('plot01') > -1) {
    color = d3.scale.ordinal()
        .range(["#116aff", "#fe8504", "#1ff7fe", "#f701ff", "#2e4a02", "#ffaad5", "#f1ff8d", "#1eff06", "#700111", "#1586c3", "#ff067d", "#0e02fb", "#1bffa1", "#921e8f", "#c49565", "#fd0128", "#4ea105", "#158279", "#c8fe0a", "#fdcc0b", "#834969", "#ff7673", "#05018b", "#c591fe", "#a6d8ab", "#948c01", "#484ba1", "#fe22c0", "#06a05d", "#694002", "#8e39e9", "#bdc6ff","#030139",  "#b33802", "#85fa60", "#a2025b", "#3e021b", "#ffcd6d", "#4a92ff", "#e564b6", "#43cfff", "#7e9051", "#e768fc", "#09406b", "#b17005", "#8fd977", "#c1063e", "#a7594f", "#14e3b8", "#bccb1e", "#53064f", "#fff1b7", "#997dba", "#fe965c", "#ffb0a7", "#046c04", "#8451ce", "#d46585", "#fef70c", "#1003c3", "#024a2e", "#0fc551", "#1f025d", "#fd5302", "#5bbfc4", "#481903", "#bfc066", "#ad04bb", "#efa425", "#06c709", "#9701ff", "#84468e", "#018da8", "#88cf01", "#6d6412", "#658a1d", "#0d3cb4", "#144cfe", "#fe5d43", "#33753e", "#4cb28f", "#e6b4ff", "#a5feef", "#caff68", "#d80f8a", "#79193a", "#97fdba", "#a85726", "#fe8cf9", "#8bfe01", "#4a315d", "#ff0155", "#02ff5e", "#6b0199", "#bc7e9f", "#fde75c"])
		.domain(labels);
}

var svg = d3.select(divToShow);
var vis = svg
	.data([data])
  .append("svg")
    .attr("width", w + margin.left + margin.right )
    .attr("height", h + margin.top + margin.bottom )
	.append("g")
	.attr("transform", "translate(" + (r + margin.left) + "," + (r + margin.top) + ")");

var donut = d3.layout.pie();
var arc = d3.svg.arc()
		.innerRadius(r * .6)
		.outerRadius(r);

var tooltip = svg
    .append("div")
	.attr("class","tooltippiechartrpt")
	.style('opacity', 1);
var tooltiplabel = tooltip.append('div')
	.attr('class','tlabel')
	.text('Total: ' + Humanize.compactInteger(tot,2));
var tooltipcount = tooltip.append('div')
	.attr('class','tcount');
var tooltippercent = tooltip.append('div')
	.attr('class','tpercent');

var arcs = vis.selectAll("path")
    .data(donut.value(function(d) { return d}))
  	.enter()
	.append("path")
		.attr("d", arc)
		.attr("class", "path")
		.attr("fill", function(d, i) { return color(labels[i]); })
		.on("mouseover", function(d,i){
			d3.select(this).attr({"stroke":d3.rgb(color).darker(),'stroke-width':1});
			// tooltip.text(labels[i]);
			tooltiplabel.text(labels[i]);
			tooltipcount.text(Humanize.compactInteger(d.value,2));
			tooltippercent.text(formatPercent(d.value/tot));
			})
		.on("mouseout", function(){
			d3.select(this).attr({"stroke":d3.rgb(color).darker(),'stroke-width':0});
			tooltiplabel.text('Total: ' + Humanize.compactInteger(tot,2));
			tooltipcount.text('');
			tooltippercent.text('');
			});

vis.append("g")
        .attr("transform", "translate(" + ( 0 ) + "," + ( -40 ) + ")")
        .append("text")
        .attr("class", "titleinpie")
        .text(title);
if (divToShow.indexOf('TaskStatus') > -1) {
    vis.append("g")
        .attr("transform", "translate(" + ( 0 ) + "," + ( -30 ) + ")")
        .append("text")
        .attr("class", "titleinpie")
        .text('by task state');
}
else if (divToShow.indexOf('TaskPriority') > -1) {
    vis.append("g")
        .attr("transform", "translate(" + ( 0 ) + "," + ( -30 ) + ")")
        .append("text")
        .attr("class", "titleinpie")
        .text('by task priority');
}

    var squareside = 10;
    if (divToShow.indexOf('TaskPriority') > -1) {
        var legend = vis.selectAll(".legend")
            .data(labels)
            .enter().append("g")
            .attr("class", "legendoutpie")
            .attr("transform", function (d, i) {
                maxLegendWidth = (i % 5) * (w+4*(margin.left+margin.right)/5)/5;
                maxLegendHeight = Math.floor(i  / 5) * 12;
                return "translate(" + (1 - r + maxLegendWidth) + ", " + (r + margin.top + 5 + maxLegendHeight) + ")";
                // return "translate(" + (margin.left - r + maxLegendWidth) + ", " + (r + margin.top + 5 + maxLegendHeight) + ")";
            });
    }
    else {
        var legend = vis.selectAll(".legend")
            .data(labels)
            .enter().append("g")
            .attr("class", "legendoutpie")
            .attr("transform", function (d, i) {
                maxLegendWidth = (i % 2) * (w + (margin.left + margin.right) / 2) / 2;
                maxLegendHeight = Math.floor(i / 2) * 15;
                return "translate(" + (margin.left - r + maxLegendWidth) + ", " + (r + margin.top + 5 + maxLegendHeight) + ")";
            });
    }

    legend.append("rect")
            .attr("x", 0)
            .attr("width", squareside)
            .attr("height", squareside)
            .style("fill", function(d) {
                return color(d);
            })
            .style({"stroke":d3.rgb(color).darker(),'stroke-width':0.4});

    legend.append("text")
            .attr("x", squareside+3)
            .attr("y", 8)
            .attr("class", 'legendpie')
            .text(function(d) {
                return d;
            });


}

function pandamonProgressBarFunc(values,divToShow,title){

var margin = {top: 20, right: 20, bottom: 60, left: 10},
    width = 400 - margin.left - margin.right,
    height = 120 - margin.top - margin.bottom;
var colors= ["#248F24", "#cccccc"];
var x = d3.scale.linear()
          .domain([0, d3.max(values)])
          .range([0, width]);
var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");
var svg = d3.select(divToShow).append("svg")
      .attr("class", "bullet")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

svg.append("rect")
          .attr("class", "target")
          .attr("width", x(values[0]))
          .attr("height", height)
          .attr("x", 0);
svg.append("rect")
          .attr("class", "forecastedprogress")
          .attr("width", x(values[2]))
          .attr("height", height-2)
          .attr("x", 1)
          .attr("y", 1);
if (values[1]>=values[2]){
svg.append("rect")
          .attr("class", "currentprogressg")
          .attr("width", x(values[1])-2)
          .attr("height", height/2-2 )
          .attr("x", 1)
          .attr("y", height/4);
}
else {
colors[0]="#FF0000";
svg.append("rect")
          .attr("class", "currentprogressb")
          .attr("width", x(values[1]-2))
          .attr("height", height/2-2 )
          .attr("x", 1)
          .attr("y", height/4);
}
svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis);

svg.append("g")
        .attr("transform", "translate(" + (width / 2) + ", -10)")
        .append("text")
        .attr("class", "title")
        .text(title);
var color = d3.scale.ordinal().range(colors).domain(['Current  progress', 'Forecasted  progress']);
var squareside = 10;
var legend = svg.selectAll(".legend")
            .data(color.domain().slice())
            .enter().append("g")
            .attr("class", "legend")
            .attr("transform", function(d, i) {
                maxLegendWidth = (i % 2) * (width+(margin.left+margin.right)/2)/2;
                maxLegendHeight = Math.floor(i  / 2) * 12;
                return "translate(" + (maxLegendWidth - margin.left/2) + ", " + (height + margin.top + maxLegendHeight + 5) + ")";
            });

    legend.append("rect")
            .attr("x", 0)
            .attr("width", squareside)
            .attr("height", squareside)
            .style("fill", color)
            .style({"stroke":d3.rgb(color).darker(),'stroke-width':0.4});

    legend.append("text")
            .attr("x", squareside+5)
            .attr("y", 10)
            .text(function(d) {
                return d;
            });

}

function pandamonTaskProfile(values, ttcflag, divToShow, title) {


var margin = {top: 40, right: 20, bottom: 80, left: 50},
    width = 600 - margin.left - margin.right,
    height = 400 - margin.top - margin.bottom;


var parseDate = d3.time.format("%Y-%m-%d %H:%M:%S").parse;
values.forEach(function(d) {
            d.endtime=parseDate(d.endtime);
            d.starttime=parseDate(d.starttime);
            d.ttctime=parseDate(d.ttctime);
        });
var data = values;

var x = d3.time.scale().range([0, width]);
var y = d3.scale.linear().range([height, 0]);

var xAxis = d3.svg.axis().scale(x)
    .orient("bottom").ticks(10);

var yAxis = d3.svg.axis().scale(y)
    .orient("left").ticks(10);
y.domain([0, d3.max(data, function(d) { return d.tobedonepct; })]);

var valueline = d3.svg.line()
    .x(function(d) { return x(d.endtime); })
    .y(function(d) { return y(d.tobedonepct); });


if (ttcflag==1) {
    var ttccoldline = d3.svg.line()
        .x(function (d) {
            return x(d.ttctime);
        })
        .y(function (d) {
            return y(d.ttccoldline);
        });
}

var svg = d3.select(divToShow)
    .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
    .append("g")
        .attr("transform",
              "translate(" + margin.left + "," + margin.top + ")");

var chart = svg.selectAll(".chart")
        .data(data)
        .enter().append("g");
    
if (ttcflag==1){
    if (values[values.length-1].endtime < values[values.length-1].ttctime) {
        x.domain(d3.extent(data, function(d) { return d.ttctime; }));
    }
    else {
        x.domain(d3.extent(data, function(d) { return d.endtime; }));
    }
}
    else {
        x.domain(d3.extent(data, function(d) { return d.endtime; }));
}

if (ttcflag==1) {
    chart.append("path")
        .attr("class", function () {
            if (values[values.length-2].tobedonepct <= values[values.length-2].ttccoldline) {
                return 'linetaskgood';
            }
            else {
                return 'linetaskbad';
            }
        })
        .attr("d", valueline(data.filter(function (d) {
            return d.tobedonepct;})));
    chart.append("path")
        .attr("class", "linettccold")
        .attr("d", ttccoldline(data));
}
    else {
    chart.append("path")
        .attr("class", "linetaskfrofile")
        .attr("d", valueline(data));
}

    svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis)
            .selectAll("text")
                .attr("dx", -32)
                .attr("dy", 5)
                .attr("transform", function(d) {
                    return "rotate(-45)"
                });
    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis);
    svg.append("g")
        .attr("transform", "rotate(-90)")
		.append("text")
        .attr("y", 0 - margin.left)
        .attr("x",0 - (height / 2))
        .attr("dy", "1em")
        .style("text-anchor", "middle")
        .text("to be done, %");
    svg.append("g")
        .attr("transform", "translate(" + (width / 2) + ", -10)")
        .append("text")
        .attr("class", "title")
        .text(title);

if (ttcflag==1) {
    var color = d3.scale.ordinal().range(['green', 'red', 'gray']).domain(['Task progress is faster than predicted', 'Task progress is slower than predicted',  'Forecasted  progress']);
}
else {
    var color = d3.scale.ordinal().range(['#1E90FF', '#116aff']).domain(['Start time', 'Task  progress']);
}
var squareside = 10;
var legend = svg.selectAll(".legend")
            .data(color.domain().slice())
            .enter().append("g")
            .attr("class", "legend")
            .attr("transform", function(d, i) {
                maxLegendWidth = (i % 3) * (width+2*(margin.left+margin.right)/3)/3;
                maxLegendHeight = Math.floor(i  / 3) * 12;
                return "translate(" + (maxLegendWidth - 2*margin.left/3) + ", " + (height + margin.top + 25 + maxLegendHeight) + ")";
            });

    legend.append("rect")
            .attr("x", 0)
            .attr("width", squareside)
            .attr("height", squareside)
            .style("fill", color)
            .style({"stroke":d3.rgb(color).darker(),'stroke-width':0.4});

    legend.append("text")
            .attr("x", squareside+5)
            .attr("y", 10)
            .text(function(d) {
                return d;
            });
}

function globalSharesPieChartFunc(values,divToShow,title){

var formatDecimal = d3.format(",.2f"),
	formatPercent = d3.format(",.1%");
let colors= [
    "#62c9ae","#52cad7","#d5a9e4","#e38924","#9bd438","#438760","#ca46ce","#e08284","#4ba930",
    "#a191d6","#57a3cf","#476be2","#85713b","#e35625","#a5be48","#a0c284","#498635","#e135ac","#d6c175","#dc82e1",
    "#7458df","#e8875c","#b36eee","#5bdd61","#c39438","#d4c926","#dd74b6","#cf4482","#9e6c28","#86cd6f","#af511c",
    "#6759bd","#a45d4d","#5c94e5","#e28fb1","#ec2c6b","#4fd08e","#9d43ba","#7a8435","#6b699b","#7f84ea","#8d5cac",
    "#c94860","#d9a276","#a05981","#cd5644","#b3439b","#4569b1","#d9b63a","#dc3238"];

var data = $.map(values, function(value, key) { if (value>0) {return value} });
var labels = $.map(values, function(value, key) { if (value>0) {return key} });
var tot = 0;
for (var i = 0; i < data.length; i++) { tot += data[i] << 0;}
var margin = {top: 30, right: 270, bottom: 30, left: 30},
    w = 700 - margin.left - margin.right,
    h = 400 - margin.top - margin.bottom,
    r = Math.min(w, h) / 2,
    color = d3.scale.ordinal()
		.range(colors).domain(labels);

var svg = d3.select(divToShow);
var vis = svg
	.data([data])
  .append("svg")
    .attr("width", w + margin.left + margin.right )
    .attr("height", h + margin.top + margin.bottom )
	.append("g")
	.attr("transform", "translate(" + (r + margin.left) + "," + (r + margin.top) + ")");

var donut = d3.layout.pie();
var arc = d3.svg.arc()
		.innerRadius(r * .6)
		.outerRadius(r);

var tooltip = svg
    .append("div")
	.attr("class","tooltippiechartgs")
	.style('opacity', 1);
var tooltiplabel = tooltip.append('div')
	.attr('class','tlabel')
	.text('Total: ' + Humanize.compactInteger(tot,2));
var tooltipcount = tooltip.append('div')
	.attr('class','tcount');
var tooltippercent = tooltip.append('div')
	.attr('class','tpercent');

var arcs = vis.selectAll("path")
    .data(donut.value(function(d) { return d}))
  	.enter()
	.append("path")
		.attr("d", arc)
		.attr("class", "path")
		.attr("fill", function(d, i) { return color(labels[i]); })
		.on("mouseover", function(d,i){
			d3.select(this).attr({"stroke":d3.rgb(color).darker(),'stroke-width':1});
			// tooltip.text(labels[i]);
			tooltiplabel.text(labels[i]);
			tooltipcount.text(Humanize.compactInteger(d.value,2));
			tooltippercent.text(formatPercent(d.value/tot));
			})
		.on("mouseout", function(){
			d3.select(this).attr({"stroke":d3.rgb(color).darker(),'stroke-width':0});
			tooltiplabel.text('Total: ' + Humanize.compactInteger(tot,2));
			tooltipcount.text('');
			tooltippercent.text('');
			});

vis.append("g")
        .attr("transform", "translate(" + ( 0 ) + "," + ( -50 ) + ")")
        .append("text")
        .attr("class", "title")
        .text(title);

    var squareside = 15;
    var legend = vis.selectAll(".legend")
            .data(color.domain().slice())
          .enter().append("g")
            .attr("class", "legendoutpie")
            .attr("transform", function(d, i) {
                maxLegendWidth =  (i % 2) * (margin.right - 20 + w - 2*r - margin.left)/2;
                maxLegendHeight = Math.floor(i/2) * 20;
                return "translate(" + (r + 20 + maxLegendWidth) + ", " + (- r + maxLegendHeight) + ")";
            });

    legend.append("rect")
            .attr("x", 0)
            .attr("width", squareside)
            .attr("height", squareside)
            .style("fill", color)
            .style({"stroke":d3.rgb(color).darker(),'stroke-width':0.4});

    legend.append("text")
            .attr("x", squareside+5)
            .attr("y", 12)
            .text(function(d) {
                return d;
            });
}

// Line chart for prodNeventsTrend page

function multiLineChartFunc(values,divToShow,title){

    var svg = d3.select(divToShow),
        margin = {top: 20, right: 220, bottom: 40, left: 60},
        width = 1400 - margin.left - margin.right,
        height = 500 - margin.top - margin.bottom;

    var formatDate = d3.time.format("%Y-%m-%d %H:%M:%S").parse;

    var x = d3.time.scale().range([0, width]),
        y = d3.scale.linear().range([height, 0]);

    if (title.indexOf('separated')>=0) {
        var color = d3.scale.ordinal()
            .range(["#ff7f0e", "#d62728", "#1f77b4", "#2ca02c", "#9467bd", "#bcbd22", "#17becf", "#e377c2", "#ffbb78", "#ff9896", "#aec7e8", "#98df8a", "#c5b0d5", "#dbdb8d", "#9edae5", "#f7b6d2", '#000000', '#7f7f7f'])
            .domain(['evgen_running', 'pile_running', 'simul_running', 'recon_running', 'reprocessing_running', 'deriv_running', 'merge_running', 'eventIndex_running','evgen_waiting', 'pile_waiting', 'simul_waiting', 'recon_waiting', 'reprocessing_waiting', 'deriv_waiting', 'merge_waiting', 'eventIndex_waiting', 'total_running', 'total_waiting']);
    }
    else {
        var color = d3.scale.ordinal()
            .range([ "#248F24", "#47D147",  "#c7c7c7"])
            .domain(['used', 'running', 'waiting']);
    }
    var line = d3.svg.line()
        .x(function(d) { return x(d.timestamp); })
        .y(function(d) { return y(d.nevents); });

    var data = values;

      // data structure:
      // [{'state1': ev_state1, 'values1': [{'date':parsed_date, 'nevents': nevents}, ...]}]

    nevents = [];
    for (var i=0; i<data.length; i++) {
        data[i].values.forEach(function (d) {
            d.timestamp = formatDate(d.timestamp);
            nevents.push(d.nevents)
        })
    }

    x.domain(d3.extent(data[0].values, function (d) { return d.timestamp; }));

    y.domain([ 0, d3.max(nevents) ]);


    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");
    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left")
        .tickFormat(hFormat);

    var vis = svg
	.data([data])
      .append("svg")
        .attr("width", width + margin.left + margin.right )
        .attr("height", height + margin.top + margin.bottom )
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    vis.append("g")
      .attr("class", "axis axis--x")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis);

    vis.append("g")
      .attr("class", "axis axis--y")
      .call(yAxis);
    vis.append("g")
        .attr("transform", "rotate(-90)")
		.append("text")
        .attr("y", 0 - margin.left)
        .attr("x",0 - (height / 2))
        .attr("dy", "1em")
        .style("text-anchor", "middle")
        .text("N events");

    vis.append("g")
        .attr("transform", "translate(" + ( (width)/2 ) + "," + ( -10 ) + ")")
        .append("text")
        .attr("class", "title")
        .text(title);

    var evlines = vis.selectAll(".state")
        .data(data)
        .enter()
            .append("g")
            .attr("class", "state");

    evlines.append("path")
      .attr("class", "linenevents")
      .attr("d", function(d) { return line(d.values); })
      .style("stroke", function(d) { return color(d.state); });


    var mouseG = vis.append("g")
      .attr("class", "mouse-over-effects");

    mouseG.append("path") // this is the black vertical line to follow mouse
      .attr("class", "mouse-line")
      .style("stroke", "black")
      .style("stroke-width", "1px")
      .style("opacity", "0");

    var lines = document.getElementsByClassName('linenevents');

    var mousePerLine = mouseG.selectAll('.mouse-per-line')
      .data(data)
      .enter()
      .append("g")
      .attr("class", "mouse-per-line");

    mousePerLine.append("circle")
      .attr("r", 7)
      .style("stroke", function(d) {
        return color(d.state);
      })
      .style("fill", "none")
      .style("stroke-width", "1px")
      .style("opacity", "0");

    mousePerLine.append("text")
      .attr("transform", "translate(10,3)");

    mouseG.append('svg:rect') // append a rect to catch mouse movements on canvas
      .attr('width', width) // can't catch mouse events on a g element
      .attr('height', height)
      .attr('fill', 'none')
      .attr('pointer-events', 'all')
      .on('mouseout', function() { // on mouse out hide line, circles and text
        d3.select(".mouse-line")
          .style("opacity", "0");
        d3.selectAll(".mouse-per-line circle")
          .style("opacity", "0");
        d3.selectAll(".mouse-per-line text")
          .style("opacity", "0");
      })
      .on('mouseover', function() { // on mouse in show line, circles and text
        d3.select(".mouse-line")
          .style("opacity", "1");
        d3.selectAll(".mouse-per-line circle")
          .style("opacity", "1");
        d3.selectAll(".mouse-per-line text")
          .style("opacity", "1");
      })
      .on('mousemove', function() { // mouse moving over canvas
        var mouse = d3.mouse(this);
        d3.select(".mouse-line")
          .attr("d", function() {
            var d = "M" + mouse[0] + "," + height;
            d += " " + mouse[0] + "," + 0;
            return d;
          });

        d3.selectAll(".mouse-per-line")
          .attr("transform", function(d, i) {
            // console.log(width/mouse[0])
            var xDate = x.invert(mouse[0]),
                bisect = d3.bisector(function(d) { return d.timestamp; }).right;
                idx = bisect(d.values, xDate);

            var beginning = 0,
                end = lines[i].getTotalLength(),
                target = null;

            while (true){
              target = Math.floor((beginning + end) / 2);
              pos = lines[i].getPointAtLength(target);
              if ((target === end || target === beginning) && pos.x !== mouse[0]) {
                  break;
              }
              if (pos.x > mouse[0])      end = target;
              else if (pos.x < mouse[0]) beginning = target;
              else break; //position found
            }

            d3.select(this).select('text')
              .text(Humanize.compactInteger(y.invert(pos.y),2));



            return "translate(" + mouse[0] + "," + pos.y +")";
          });
      });

    var states = data.map(function (d) {
        return d.state;
    });

    var squareside = 15;
    var legend = vis.selectAll(".legend")
            .data(states.sort())
          .enter().append("g")
            .attr("class", "legendoutpie")
            .attr("transform", function(d, i) {
                maxLegendWidth = 65;
                maxLegendHeight = Math.floor(i) * 20;
                return "translate(" + (width + maxLegendWidth) + ", " + (maxLegendHeight) + ")";
            });

    legend.append("rect")
            .attr("x", 0)
            .attr("width", squareside)
            .attr("height", squareside)
            .style("fill", color)
            .style({"stroke":d3.rgb(color).darker(),'stroke-width':0.4});

    legend.append("text")
            .attr("x", squareside+5)
            .attr("y", 12)
            .text(function(d) {
                return d;
            });

}


function taskInfoPieChartFunc(values,divToShow,title){

var formatDecimal = d3.format(",.2f"),
	formatPercent = d3.format(",.1%");
var data = $.map(values, function(value, key) { if (value>0) {return value} });
var labels = $.map(values, function(value, key) { if (value>0) {return key} });
var tot = 0;
for (var i = 0; i < data.length; i++) { tot += data[i];}
var margin = {top: 20, right: 200, bottom: 90, left: 5},
    w = 600 - margin.left - margin.right,
    h = 400 - margin.top - margin.bottom,
    r = Math.min(w, h) / 2;

if (title.indexOf('events') > -1) {
    color = d3.scale.ordinal()
        .range(["#116aff", "#fe8504", "#1ff7fe", "#f701ff", "#2e4a02", "#ffaad5", "#f1ff8d", "#1eff06", "#700111", "#1586c3", "#ff067d", "#0e02fb", "#1bffa1", "#921e8f", "#c49565", "#fd0128", "#4ea105", "#158279", "#c8fe0a", "#fdcc0b", "#834969", "#ff7673", "#05018b", "#c591fe", "#a6d8ab", "#948c01", "#484ba1", "#fe22c0", "#06a05d", "#694002", "#8e39e9", "#bdc6ff","#030139",  "#b33802", "#85fa60", "#a2025b", "#3e021b", "#ffcd6d", "#4a92ff", "#e564b6", "#43cfff", "#7e9051", "#e768fc", "#09406b", "#b17005", "#8fd977", "#c1063e", "#a7594f", "#14e3b8", "#bccb1e", "#53064f", "#fff1b7", "#997dba", "#fe965c", "#ffb0a7", "#046c04", "#8451ce", "#d46585", "#fef70c", "#1003c3", "#024a2e", "#0fc551", "#1f025d", "#fd5302", "#5bbfc4", "#481903", "#bfc066", "#ad04bb", "#efa425", "#06c709", "#9701ff", "#84468e", "#018da8", "#88cf01", "#6d6412", "#658a1d", "#0d3cb4", "#144cfe", "#fe5d43", "#33753e", "#4cb28f", "#e6b4ff", "#a5feef", "#caff68", "#d80f8a", "#79193a", "#97fdba", "#a85726", "#fe8cf9", "#8bfe01", "#4a315d", "#ff0155", "#02ff5e", "#6b0199", "#bc7e9f", "#fde75c"])
		.domain(labels);
}

var svg = d3.select(divToShow);
var vis = svg
	.data([data])
  .append("svg")
    .attr("width", w + margin.left + margin.right )
    .attr("height", h + margin.top + margin.bottom )
	.append("g")
	.attr("transform", "translate(" + (r + margin.left) + "," + (r + margin.top) + ")");

var donut = d3.layout.pie();
var arc = d3.svg.arc()
		.innerRadius(r * .6)
		.outerRadius(r);

var tooltip = svg
    .append("div")
	.attr("class","tooltippiecharttaskinfo")
	.style('opacity', 1);
var tooltiplabel = tooltip.append('div')
	.attr('class','tlabel')
	.text('Total: ' + Humanize.compactInteger(tot,2));
var tooltipcount = tooltip.append('div')
	.attr('class','tcount');
var tooltippercent = tooltip.append('div')
	.attr('class','tpercent');

var arcs = vis.selectAll("path")
    .data(donut.value(function(d) { return d}))
  	.enter()
	.append("path")
		.attr("d", arc)
		.attr("class", "path")
		.attr("fill", function(d, i) { return color(labels[i]); })
		.on("mouseover", function(d,i){
			d3.select(this).attr({"stroke":d3.rgb(color).darker(),'stroke-width':1});
			// tooltip.text(labels[i]);
			tooltiplabel.text(labels[i]);
			tooltipcount.text(Humanize.compactInteger(d.value,2));
			tooltippercent.text(formatPercent(d.value/tot));
			})
		.on("mouseout", function(){
			d3.select(this).attr({"stroke":d3.rgb(color).darker(),'stroke-width':0});
			tooltiplabel.text('Total: ' + Humanize.compactInteger(tot,2));
			tooltipcount.text('');
			tooltippercent.text('');
			});

vis.append("g")
        .attr("transform", "translate(" + ( 0 ) + "," + ( -155 ) + ")")
        .append("text")
        .attr("class", "title")
        .text(title);


    var squareside = 10;
    var legend = vis.selectAll(".legend")
        .data(labels)
        .enter().append("g")
        .attr("class", "legendoutpie")
        .attr("transform", function (d, i) {
            maxLegendWidth = margin.right;
            maxLegendHeight = Math.floor(i) * 12;
            return "translate(" + (margin.left + r + 5 ) + ", " + (-r + margin.top + maxLegendHeight) + ")";
        });


    legend.append("rect")
            .attr("x", 0)
            .attr("width", squareside)
            .attr("height", squareside)
            .style("fill", function(d) {
                return color(d);
            })
            .style({"stroke":d3.rgb(color).darker(),'stroke-width':0.4});

    legend.append("text")
            .attr("x", squareside+3)
            .attr("y", 8)
            .attr("class", 'legend')
            .text(function(d) {
                return d;
            });
}
