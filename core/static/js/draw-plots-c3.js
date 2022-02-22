

var task_state_colors =  {
    'done': '#165616',
    'finished': '#207f20',
    'running': '#47D147',
    'waiting': '#c7c7c7',
    'assigning': '#099999',
    'exhausted': '#FF9933',
    'paused': '#808080',
    'throttled': '#FF9933',
    'pending': '#deb900',
    'ready': '#099999',
    'registered': '#4a4a4a',
    'scouting': '#addf80',
    'scouted': '#addf80',
    'toabort': '#ff9896',
    'aborting': '#FF8174',
    'aborted': '#ff0000',
    'failed': '#ff0000',
    'broken': '#b22222',
    'passed': '#1a1a1a',
    'defined': '#2174bb',
    'remaining': '#2174bb',
    'rerefine': '#4a4a4a',
    'prepared': '#4a4a4a',
};


// Replace the confusing G (for Giga) with  the more recognizable B (for Billion) in default SI prefixes.
function hFormat(num) {
    var siFormat = d3.format(",.3s");
return siFormat(num).replace("G", "B");
}

function getWidth() {
  return Math.min(
    document.body.scrollWidth,
    document.documentElement.scrollWidth,
    document.body.offsetWidth,
    document.documentElement.offsetWidth,
    document.documentElement.clientWidth
  );
}

function getScreenCategory(width) {
  let scrnCat = '';
  let breakpoints = {
    small: [0, 640],
    medium: [640, 1280],
    large: [1280, 99999],
  };
  for (const item in breakpoints) {
    if (width >= breakpoints[item][0] && width < breakpoints[item][1]) {
      scrnCat = item;
    }
  }
  (scrnCat.length === 0) ? scrnCat = 'small' : scrnCat;

  return scrnCat
}

function getPlotWidth() {
  // calculate plot width
  let padding = 20;
  let nplot_thresholds = [
    {page_width_px: 1800, n_plots: 3},
    {page_width_px: 1300, n_plots: 2},
    {page_width_px: 800, n_plots: 1},
  ];
  let page_width = getWidth();
  let nplots = 3;
  let i = 0;
  while (i <= nplot_thresholds.length && page_width < nplot_thresholds[i].page_width_px) {
    nplots = nplot_thresholds[i].n_plots;
    i++;
  }
  return page_width/nplots - padding;
}

var formatStats = d3.format(".3s");

function draw_donut(data, divid, title, ext={}) {
    let colors = {};
    if (ext.colors === 'states') {
        colors = {
            actual: '#ff7f0e',
            running: '#1f77b4',
            finished: '#2ca02c',
            failed: '#d62728',
        }
    }
    else if (ext.colors !== null && typeof ext.colors === 'object') {
        colors = ext.colors;
    }
    else if (title.indexOf('status') > -1) {
        colors =  {
            'done': '#165616',
            'finished': '#207f20',
            'running': '#34a934',
            'waiting': '#c7c7c7',
            'assigning': '#099999',
            'exhausted': '#FF9933',
            'paused': '#808080',
            'throttled': '#deb900',
            'pending': '#a9a9a9',
            'ready': '#3b8e67',
            'registered': '#4a4a4a',
            'scouting': '#addf80',
            'aborting': '#ff9896',
            'aborted': '#FF8174',
            'failed': '#ff0000',
            'broken': '#b22222',
            'passed': '#1a1a1a',
            'defined': '#2174bb',
            'remaining': '#2174bb',
        };
    }
    else {
       let colors_all = [
        "#62c9ae","#52cad7","#d5a9e4","#e38924","#9bd438","#438760","#ca46ce","#e08284","#4ba930",
        "#a191d6","#57a3cf","#476be2","#85713b","#e35625","#a5be48","#a0c284","#498635","#e135ac","#d6c175","#dc82e1",
        "#7458df","#e8875c","#b36eee","#5bdd61","#c39438","#d4c926","#dd74b6","#cf4482","#9e6c28","#86cd6f","#af511c",
        "#6759bd","#a45d4d","#5c94e5","#e28fb1","#ec2c6b","#4fd08e","#9d43ba","#7a8435","#6b699b","#7f84ea","#8d5cac",
        "#c94860","#d9a276","#a05981","#cd5644","#b3439b","#4569b1","#d9b63a","#dc3238"];
       for (let i=0; i<data.length; i++) {colors[data[i][0]] = colors_all[i]}
    }
    let width = 600;
    let height = 300;
    if (ext.size) {
        width = ext.size[0];
        height = ext.size[1];
    }
    let legend_position = 'right';
    if (ext.legend_position) {legend_position = ext.legend_position;}

    var plot = c3.generate({
        bindto: '#' + divid,
        data: {
            columns: data,
            type : 'donut',
            colors: colors,
        },
        donut: {
            title: title,
            label: {
                format: function (d) { return hFormat(d);},
                threshold: 0.08,
            },
        },
        tooltip: {
            format: {
                value: function (value, ratio, id) {
                    return hFormat(value)  + ' [' + d3.format(".1%")(ratio) + ']';
                }
            }
        },
        legend: {
            position: legend_position,
            show: true
        },
        size: {
            width: width,
            height: height,
        },
    });
    return plot
}

function draw_bar(data, divid, title, ext) {
    let width = 300;
    let height = 200;
    if (ext.size) {width = ext.size[0]; height=ext.size[1]}
    let colors = {};
    let axis_labels = ['', ''];
    if (ext.labels) {axis_labels = ext.labels;}
    var chart = c3.generate({
        bindto: '#' + divid,
        data: {
            x: data[0][0],
            columns: data,
            type: 'bar',
            colors: colors,
        },
        padding: {
          right: 20
        },
        bar: {
            width: {
                ratio: 0.6
            }
        },
        legend: {
             show: false
        },
        axis: {
            x: {
                label: {
                    text: axis_labels[0],
                    position: 'outer-right',
                },
                type: 'category',
                tick: {
                  count: width / 20,
                  multiline: false,
                }
            },
            y: {
                label: {
                  text: axis_labels[1],
                  position: 'outer-middle'
                }
            }
        },
        size: {
            width: width,
            height: height,
        },
        title: {
          text: title
        },

    });

    if (ext.stats) {
      let statistics = [
        {type: "\u03BC", val: formatStats(ext.stats[0])},
        {type: "\u03C3", val: formatStats(ext.stats[1])},
        ];
      var chart_svg = d3.select('#' + divid + " svg");
      var statlegend = chart_svg.selectAll(".statlegend")
        .data(statistics)
        .enter()
        .append("g")
        .attr("class", "statlegend")
        .attr("transform", function (d, i) {
          return "translate(" + (width - 40) + ", " + ((i + 1) * 15) + ")";
        });

      statlegend.append("text")
        .attr("x", 0)
        .attr("y", 0)
        .attr("class", "stattext")
        .text(function (d) {
          return d.type + "=" + d.val;
        });
    }
    return chart
}


function draw_bar_timeseries(data, divid, ext) {
    let width = 300;
    let height = 200;
    if (ext.size) {width = ext.size[0]; height=ext.size[1]}
    let colors = {};
    if (ext.colors !== null && typeof ext.colors === 'object') {
      colors = ext.colors;
    }
    let timeFormat = '%Y-%m-%d %H:%M:%S';
    if (ext.timeFormat) {timeFormat = ext.timeFormat;}
    let axis_labels = ['', ''];
    if (ext.labels) {axis_labels = ext.labels;}
    let title = '';
    if (ext.title) {title = ext.title;}
    var chart = c3.generate({
        bindto: '#' + divid,
        data: {
            x: data[0][0],
            xFormat: timeFormat,
            columns: data,
            type: 'bar',
            colors: colors,
        },
        title: {
            text: title,
        },
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    format: timeFormat,
                    rotate: -30,
                },
                label: {
                    text: axis_labels[0],
                    position: 'outer-right'
                },
            },
            y: {
                label: {
                  text: axis_labels[1],
                  position: 'outer-middle'
                }
            }
        },
        legend: {
             show: false
        },
        size: {
            width: width,
            height: height,
        },

    });
    return chart
}

function draw_bar_cat(data, divid, title, ext) {
    let width = 300;
    let height = 200;
    let labels = ['',''];
    if (ext.size) {width = ext.size[0]; height=ext.size[1]}
    if (ext.labels) {labels = ext.labels}
    // increase height if x axis labels are long str
    let cat_names = [...data[0]];
    cat_names.shift();
    cat_names.sort(function (a,b) {return b.length - a.length;});
    if (cat_names[0].length >= 10) {height += 5 * (cat_names[0].length - 10);}

    let colors = {};
    if (ext.colors === 'gs') {
        colors = {
            Actual: '#1f77b4',
            Target: '#2ca02c',
        }
    }
    else if (ext.colors !== null && typeof ext.colors === 'object') {
        colors = ext.colors;
    }

    let groups = [];
    if (ext.grouped) {groups = data.slice(1).map(d => d[0]);}

    let legend = false;
    if (ext.legend) {legend = ext.legend;}

    let y_grid_lines = [];
    if (ext.y_grid_lines) {y_grid_lines = ext.y_grid_lines;}


    var chart = c3.generate({
        bindto: '#' + divid,
        data: {
            x: data[0][0],
            columns: data,
            type: 'bar',
            colors: colors,
            groups: [groups],
        },
        padding: {
          right: 20
        },
        bar: {
            width: {
                ratio: 0.6
            }
        },
        grid: {
          y: {
            lines: y_grid_lines,
          }
        },
        legend: {
             show: legend,
        },
        axis: {
            x: {
                type: 'category',
                tick: {
                    rotate: -60,
                    multiline: false,
                },
                label: {
                    text: labels[0],
                    position: 'outer-right'
                },
            },
            y: {
                tick: {
                    format: function (d) { return hFormat(d); }
                },
                label: {
                  text: labels[1],
                  position: 'outer-middle'
                }
            }
        },
        size: {
            width: width,
            height: height,
        },
        title: {
          text: title
        },

    });
    return chart
}

function draw_sbar(data, divid, title, ext) {
    let width = 300;
    let height = 200;
    let labels = ['',''];
    let x = Object.keys(data[0])[0];
    let values = Object.keys(data[0])[1];
    if (ext.size) {width = ext.size[0]; height=ext.size[1]}
    if (ext.labels) {labels = ext.labels}
    let colors = {};
    if (ext.colors === 'gs') {
        colors = {
            Actual: '#1f77b4',
            Target: '#2ca02c',
        }
    }
    if ('struct' in ext) {
        x = ext.struct[0];
        values = [ext.struct[1], ext.struct[2]];
    }
    var chart = c3.generate({
        bindto: '#' + divid,
        data: {
            json: data,
            keys: {
              x: x,
              value: values,
            },
            type: 'bar',
            colors: colors,
        },
        padding: {
          right: 20
        },
        bar: {
            width: {
                ratio: 0.6
            }
        },
        legend: {
             show: false
        },
        axis: {
            x: {
                type: 'category',
                tick: {
                    rotate: -60,
                    multiline: false,
                }
            },
            y: {
                tick: {
                    format: function (d) { return d3.format(',.3s')(d); }
                },
                label: {
                  text: labels[1],
                  position: 'outer-middle'
                }
            }
        },
        size: {
            width: width,
            height: height,
        },
        title: {
          text: title
        },

    });
    return chart
}


function draw_stacked_bar_hist(rawdata, details, divToShow)  {
    var formatXAxis = d3.format(".2s");
    var formatStats = d3.format(".3s");
    var colors = [
        "#62c9ae","#52cad7","#d5a9e4","#e38924","#9bd438","#438760","#ca46ce","#e08284","#4ba930",
        "#a191d6","#57a3cf","#476be2","#85713b","#e35625","#a5be48","#a0c284","#498635","#e135ac","#d6c175","#dc82e1",
        "#7458df","#e8875c","#b36eee","#5bdd61","#c39438","#d4c926","#dd74b6","#cf4482","#9e6c28","#86cd6f","#af511c",
        "#6759bd","#a45d4d","#5c94e5","#e28fb1","#ec2c6b","#4fd08e","#9d43ba","#7a8435","#6b699b","#7f84ea","#8d5cac",
        "#c94860","#d9a276","#a05981","#cd5644","#b3439b","#4569b1","#d9b63a","#dc3238"];

    let statistics = [{type: "\u03BC", val:formatStats(rawdata['stats'][0])}];
	  statistics.push({type:"\u03C3", val:formatStats(rawdata['stats'][1])});

	  let data = rawdata['columns'];

	  // make list of keys to group bars into stacks
    var keys = [];
    data.forEach(function (row, index) {
        if (index >= 1) { keys.push(row[0]); }
    });

	  let is_interactive = true;
    let legend_height = 0;
    // if length of legend strings is big -> increase height of legend
    if (data.map(a => a[0].length).reduce((a,b) => a + b) > 50)  {
        legend_height = 15 * (Math.floor(data.map(a => a[0].length).reduce((a,b) => a + b) / 50 ) - 1);
    }
    // if number of legend items is large -> show only top 20
    let data_legend_to_hide = [];
    if (data.length > 20) {
        // disable interactivity for plot having a lot of data
        is_interactive = false;
        data.forEach(function (row, index) {
            if (index >= 20) { data_legend_to_hide.push(row[0]); }
        });
        legend_height = Math.max(15 * (data.length - data_legend_to_hide.length) / 4, legend_height*20/data.length);
        if (details.title.indexOf('[only top-20') < 0) {details.title += ' [only top-20 items listed in legend]'}
    }

    let width = getPlotWidth();
    let height = 300 + legend_height;

    let options = {
        bindto: '#' + divToShow,
        data: {
            x: data[0][0],
            columns: data,
            type: 'bar',
            groups: [keys]
        },
        color: {
            pattern: colors,
        },
        bar: {
            width: {
                ratio: 0.8,
            }
        },
        title: {
            text: details.title,
        },
        axis: {
            x: {
                tick: {
                    type: 'category',
                    format: function (d) {return formatXAxis(d);},
                },
                label: {
                    text: details.xlabel,
                    position: 'outer-right'
                }
            },
            y: {
                tick: {
                    format: function (d) { return d; }
                },
                label: {
                  text: 'Number of jobs',
                  position: 'outer-middle'
                }
            }
        },
        legend: {
            hide: data_legend_to_hide,
        },
        tooltip: {
          show: is_interactive,
          format: {
            title: function (x, index) { return  formatStats(x); }
          }
        },
        interaction: {
          enabled: is_interactive,
        },
        transition: {
            duration: 0
        },
        size: {
            width: width,
            height: height,
        },
        padding: {
            right: 40,
        },
    };
    if (is_interactive === false) {
        options.legend.item = {
            onclick: function (id) {},
            onmouseover: function (id) {},
            onmouseout: function (id) {},
        };

    }
    var chart = c3.generate(options);

    var chart_svg = d3.select('#' + divToShow + " svg");
    var statlegend = chart_svg.selectAll(".statlegend")
        .data(statistics)
        .enter()
        .append("g")
        .attr("class", "statlegend")
        .attr("transform", function (d, i) {
            return "translate(" + (width - 40) + ", " + (15 + (i + 1) * 15) + ")";
        });

    statlegend.append("text")
        .attr("x", 0)
        .attr("y", 0)
        .attr("class", "stattext")
        .text(function (d) {
            return d.type + "=" + d.val;
        });
    return chart
}


function draw_bar_hist(rawdata, divToShow)  {
    var colors = [
        "#62c9ae","#52cad7","#d5a9e4","#e38924","#9bd438","#438760","#ca46ce","#e08284","#4ba930",
        "#a191d6","#57a3cf","#476be2","#85713b","#e35625","#a5be48","#a0c284","#498635","#e135ac","#d6c175","#dc82e1",
        "#7458df","#e8875c","#b36eee","#5bdd61","#c39438","#d4c926","#dd74b6","#cf4482","#9e6c28","#86cd6f","#af511c",
        "#6759bd","#a45d4d","#5c94e5","#e28fb1","#ec2c6b","#4fd08e","#9d43ba","#7a8435","#6b699b","#7f84ea","#8d5cac",
        "#c94860","#d9a276","#a05981","#cd5644","#b3439b","#4569b1","#d9b63a","#dc3238"];

    var formatXAxis = d3.format(".2s");
    var formatStats = d3.format(".3s");

    var values = rawdata['sites'];
    var ranges = rawdata['ranges'];
    var stats = rawdata['stats'];
    var details = rawdata['details'];

    let statistics = [{type: "\u03BC", val:formatStats(stats[0])}];
	statistics.push({type:"\u03C3", val:formatStats(stats[1])});

	var data = [];
    var keys = [];
    Object.keys(values).forEach(function(key) {
        keys.push(key);
        var bins = [];
        values[key].forEach(function (d) {
            bins.push(parseInt(d,10))
        });
        bins.unshift(key);
        data.push(bins)
    });

    // order by most significant sites
    data.sort(function (c,d) {
        let array2 = d.slice();
        let array1 = c.slice();
        let item2 = array2.shift();
        item2 = array2.reduce((a, b) => a + b, 0);
        let item1 = array1.shift();
        item1 = array1.reduce((a, b) => a + b, 0);
        return item2 - item1;
    });

    // add x axis ranges to plot data
    var x_data = ['x'];
    ranges.forEach(function(i) {x_data.push(i)});
    data.unshift(x_data);


    let is_interactive = true;
    let legend_height = 0;
    let data_legend_to_hide = [];
    if (data.length > 20) {
        // disable interactivity for plot having a lot of data
        is_interactive = false;
        data.forEach(function (row, index) {
            if (index >= 20) { data_legend_to_hide.push(row[0]); }
        });
        legend_height = 15 * (data.length - data_legend_to_hide.length) / 4;
        details.title += ' [only top-20 sites listed in legend]'
    }
    let width = getPlotWidth();
    let height = 300 + legend_height;

    var chart = c3.generate({
        bindto: '#' + divToShow,
        data: {
            x: data[0][0],
            columns: data,
            type: 'bar',
            groups: [keys]
        },
        color: {
            pattern: colors,
        },
        bar: {
            width: {
                ratio: 0.8,
            }
        },
        title: {
            text: details.title,
        },
        axis: {
            x: {
                tick: {
                    type: 'category',
                    format: function (d) {return formatXAxis(d);},
                },
                label: {
                    text: details.xlabel,
                    position: 'outer-right'
                }
            },
            y: {
                tick: {
                    format: function (d) { return d; }
                },
                label: {
                  text: 'Number of jobs',
                  position: 'outer-middle'
                }
            }
        },
        legend: {
            hide: data_legend_to_hide,
        },
        tooltip: {
          show: is_interactive,
          format: {
            title: function (x, index) { return  formatStats(x); }
          }
        },
        interaction: {
          enabled: is_interactive,
        },
        transition: {
            duration: 0
        },
        size: {
            width: width,
            height: height,
        },
        padding: {
            right: 40,
        },
    });

    var chart_svg = d3.select('#' + divToShow + " svg");
    var statlegend = chart_svg.selectAll(".statlegend")
        .data(statistics)
        .enter()
        .append("g")
        .attr("class", "statlegend")
        .attr("transform", function (d, i) {
            return "translate(" + (width - 40) + ", " + (15 + (i + 1) * 15) + ")";
        });

    statlegend.append("text")
        .attr("x", 0)
        .attr("y", 0)
        .attr("class", "stattext")
        .text(function (d) {
            return d.type + "=" + d.val;
        });
    return chart
}


function draw_line_chart(rawdata, divid, ext={}) {
    var formatXAxis = d3.format(".0f");

    let width = getWidth()-20;
    let height = 320;
    if (ext.size) {width = ext.size[0]; height=ext.size[1];}

    let data = rawdata['data'];
    let details = rawdata['details'];
    let grid = {}
    if ('grid' in rawdata) { grid = rawdata['grid']; height += 50; }

    var chart = c3.generate({
        bindto: '#' + divid,
        data: {
            x: data[0][0],
            columns: data,
            type: 'line',
        },
        grid: grid,
        point: {
            show: false,
        },
        title: {
            text: details.title,
        },
        axis: {
            x: {
                label: {
                    text: details.xlabel,
                    position: 'outer-right'
                },
                tick: {
                  count: width/10,
                  format: function (d) {return formatXAxis(d);},
                }
            },
            y: {
                max: details.ymax,
                padding: {
                  bottom: 0,
                },
                tick: {
                    format: function (d) { return d; }
                },
                label: {
                  text: details.ylabel,
                  position: 'outer-middle',
                }
            }
        },
        tooltip: {
          format: {
            title: function (x) { return details.xlabel + ': ' + x; }
          }
        },
        transition: {
            duration: 0
        },
        size: {
            width: width,
            height: height,
        },
        padding: {
          right: 20,
        },
    });

    if ('grid' in rawdata) {
      d3.selectAll('.c3-xgrid-line').each(function(d, i){
          // for each 'text' element within the group move to the right side of a vertical line
          d3.select(this).select('text').each(function(d, i){
              d3.select(this).attr('dy', 8);
          })
      });
    }

    return chart
}


function draw_area_chart(rawdata, divid, ext={}) {
    var formatXAxis = d3.format(".0f");

    let width = getWidth()-20;
    let height = 300;
    if (ext.size) {width = ext.size[0]; height=ext.size[1];}

    let data = rawdata['data'];
    let details = rawdata['details'];
    let grid = {}
    if ('grid' in rawdata) { grid = rawdata['grid']; height += 50; }

    let keys = [];
    data.forEach(function (row) {
      keys.push(row[0]);
    });
    keys.unshift('x');



    var chart = c3.generate({
        bindto: '#' + divid,
        data: {
            x: data[0][0],
            columns: data,
            type: 'area',
            groups: [keys]
        },
        grid: grid,
        point: {
            show: false,
        },
        title: {
            text: details.title,
        },
        axis: {
            x: {
                label: {
                    text: details.xlabel,
                    position: 'outer-right'
                },
                tick: {
                  count: width/10,
                  format: function (d) {return formatXAxis(d);},
                }
            },
            y: {
                max: details.ymax,
                padding: {
                  bottom: 0,
                },
                tick: {
                    format: function (d) { return d; }
                },
                label: {
                  text: details.ylabel,
                  position: 'outer-middle',
                }
            }
        },
        tooltip: {
          format: {
            title: function (x) { return details.xlabel + ': ' + x; }
          }
        },
        transition: {
            duration: 0
        },
        size: {
            width: width,
            height: height,
        },
        padding: {
          right: 20,
        },
    });
    if ('grid' in rawdata) {
      d3.selectAll('.c3-xgrid-line').each(function(d, i){
          // for each 'text' element within the group move to the right side of a vertical line
          d3.select(this).select('text').each(function(d, i){
              d3.select(this).attr('dy', 8);
          })
      });
    }
    return chart
}


function draw_multi_line(data, divid, title, ext) {
    let width = 500;
    let height = 200;
    if (ext.size) {width = ext.size[0]; height=ext.size[1]}
    let colors = {};
    if (ext.colors) {colors = ext.colors;}
    let axis_labels = [data[0][0], ''];
    if (ext.labels) {axis_labels = ext.labels;}
    var chart = c3.generate({
        bindto: '#' + divid,
        data: {
            x: data[0][0],
            xFormat: '%Y-%m-%d %H:%M:%S',
            columns: data,
            type: 'line',
            colors: colors,
        },
        point: {
            show: false,
        },
        title: {
            text: title,
        },
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    format: '%Y-%m-%d %H:%M:%S',
                    rotate: -30,
                    count: width/10,
                },
                label: {
                    text: axis_labels[0],
                    position: 'outer-right'
                },
            },
            y: {
                // min: 0,
                padding: {
                  bottom: 0,
                },
                tick: {
                    format: function (d) { return hFormat(d); }
                },
                label: {
                  text: axis_labels[1],
                  position: 'outer-middle',
                }
            }
        },
        transition: {
            duration: 0
        },
        size: {
            width: width,
            height: height,
        },
        padding: {
          right: 20,
        },
    });
    return chart
}

function draw_scatter(data, divid, title, ext) {
    let width = 500;
    let height = 200;
    if (ext.size) {width = ext.size[0]; height=ext.size[1]}
    let colors = {};
    if (ext.colors) {
        if (typeof ext.colors === 'string' || ext.colors instanceof String)  {colors = task_state_colors;}
        else if (typeof ext.colors === 'object') {colors = ext.colors;}
    }
    let axis_labels = ['', ''];
    if (ext.labels) {axis_labels = ext.labels;}
    var chart = c3.generate({
        bindto: '#' + divid,
        data: {
            xs: ext.xs,
            xFormat: '%Y-%m-%d %H:%M:%S',
            columns: data,
            type: 'scatter',
            colors: colors,
        },
        point: {
            r: 5,
        },
        title: {
            text: title,
        },
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    format: '%Y-%m-%d %H:%M:%S',
                    rotate: -30,
                    // count: width/10,
                },
                label: {
                    text: axis_labels[0],
                    position: 'outer-right'
                },
            },
            y: {
                // min: 0,
                padding: {
                  bottom: 0,
                },
                label: {
                  text: axis_labels[1],
                  position: 'outer-middle',
                }
            }
        },
        transition: {
            duration: 0
        },
        tooltip: {
            show: true,
        },
        size: {
            width: width,
            height: height,
        },
        padding: {
          right: 20,
          left: 100,
        },
    });
    return chart
}

function draw_steps(data, divid, title, ext) {
    let width = 500;
    let height = 200;
    if (ext.size) {width = ext.size[0]; height=ext.size[1]}
    let colors = {};
    if (ext.colors) {
        if (typeof ext.colors === 'string' || ext.colors instanceof String)  {colors = task_state_colors;}
        else if (typeof ext.colors === 'object') {colors = ext.colors;}
    }
    let axis_labels = ['', ''];
    if (ext.labels) {axis_labels = ext.labels;}
    var chart = c3.generate({
        bindto: '#' + divid,
        data: {
            x: data[0][0],
            xFormat: '%Y-%m-%d %H:%M:%S',
            columns: data,
            type: 'area-step',
            colors: colors,
        },
        line: {
            step: {
                type: 'step-before',
            }
        },
        title: {
            text: title,
        },
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    format: '%Y-%m-%d %H:%M:%S',
                    rotate: -30,
                    // count: width/10,
                },
                label: {
                    text: axis_labels[0],
                    position: 'outer-right'
                },
            },
            y: {
                // min: 0,
                padding: {
                  bottom: 0,
                },
                label: {
                  text: axis_labels[1],
                  position: 'outer-middle',
                }
            }
        },
        transition: {
            duration: 0
        },
        size: {
            width: width,
            height: height,
        },
        padding: {
          right: 20,
          left: 100,
        },
    });
    return chart
}