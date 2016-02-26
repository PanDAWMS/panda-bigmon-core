/**
 * Created by spadolski on 12/22/15.
 */

function pandamonplotFunc(values, sites, divToShow, title, numberofbins) {

    colors= ["#116aff", "#fe8504", "#1ff7fe", "#f701ff", "#2e4a02", "#ffaad5", "#f1ff8d", "#1eff06", "#700111", "#1586c3", "#ff067d", "#0e02fb", "#1bffa1", "#921e8f", "#c49565", "#fd0128", "#4ea105", "#158279", "#c8fe0a", "#fdcc0b", "#834969", "#ff7673", "#05018b", "#c591fe", "#a6d8ab", "#948c01", "#484ba1", "#fe22c0", "#06a05d", "#694002", "#8e39e9", "#bdc6ff","#030139",  "#b33802", "#85fa60", "#a2025b", "#3e021b", "#ffcd6d", "#4a92ff", "#e564b6", "#43cfff", "#7e9051", "#e768fc", "#09406b", "#b17005", "#8fd977", "#c1063e", "#a7594f", "#14e3b8", "#bccb1e", "#53064f", "#fff1b7", "#997dba", "#fe965c", "#ffb0a7", "#046c04", "#8451ce", "#d46585", "#fef70c", "#1003c3", "#024a2e", "#0fc551", "#1f025d", "#fd5302", "#5bbfc4", "#481903", "#bfc066", "#ad04bb", "#efa425", "#06c709", "#9701ff", "#84468e", "#018da8", "#88cf01", "#6d6412", "#658a1d", "#0d3cb4", "#144cfe", "#fe5d43", "#33753e", "#4cb28f", "#e6b4ff", "#a5feef", "#caff68", "#d80f8a", "#79193a", "#97fdba", "#a85726", "#fe8cf9", "#8bfe01", "#4a315d", "#ff0155", "#02ff5e", "#6b0199", "#bc7e9f", "#fde75c"];

    var formatCount = d3.format(",.0f");

    var margin = {top: 30, right: 50, bottom: 220, left: 70},
        width = 650 - margin.left - margin.right,
        height = 500 - margin.top - margin.bottom;

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

    var data = [{'value': 0, 'site': ''}];

    for(var i = 0, ii = values.length; i<ii; i++) {
        data[i]={'value': values[i], 'site': sites[i]}
    }

    var svg = d3.select(divToShow)
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

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

    var color = d3.scale.ordinal().range(colors);

    y.domain([0, d3.max(stackedHistData[stackedHistData.length - 1].values, function(d) {
            return d.y + d.y0;
        })]);

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

    svg.append("g")
            .attr("class", "y axis")
            .call(yAxis);

    svg.append("g")
            .attr("transform", "translate(" + (width/2) + ", -10)")
            .append("text")
            .attr("class", "title")
            .text(title);


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

}

function pandamonProdRunTaskSumPlotFunc(values,divToShow,title){

    var formatCount = d3.format(",.0f");

    var margin = {top: 30, right: 30, bottom: 40, left: 60},
        width = 700 - margin.left - margin.right,
        height = 300 - margin.top - margin.bottom;

    var lowerBand = d3.min(values);
    var upperBand = d3.max(values);

    var x = d3.scale.linear()
        .domain([lowerBand, upperBand])
        .range([0, width])
        .nice();

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");

    var data = d3.layout.histogram()
        .bins(x.ticks(40))
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
        .attr("width", x(data[0].dx) - 1)
        .attr("height", function (d) {
            return height - y(d.y);
        });

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis);

    svg.append("g")
        .attr("transform", "translate(" + (width / 2) + ", 15)")
        .append("text")
        .attr("class", "title")
        .text(title);

}

function createGauge(divToShow, label) {
    var config =
			{
				size: 250,
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

		this.config.size = this.config.size * 0.9;

		this.config.raduis = this.config.size * 0.8 / 2;
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
                            .attr("transform", "translate(" + 0 + "," + (0.09*this.config.size) + ")");

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

		var fontSize = Math.round(this.config.size / 18);
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
									.attr("y", this.config.size - this.config.cy / 3 - fontSize)
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

var data = $.map(values, function(value, key) { if (value>0) {return value/1000000} });
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
        .attr("class", "title")
        .text(neventstot+'M events ');
}