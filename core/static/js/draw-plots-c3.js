
function draw_donut(data, divid, title, ext={}) {
    let colors_all = [
        "#62c9ae","#52cad7","#d5a9e4","#e38924","#9bd438","#438760","#ca46ce","#e08284","#4ba930",
        "#a191d6","#57a3cf","#476be2","#85713b","#e35625","#a5be48","#a0c284","#498635","#e135ac","#d6c175","#dc82e1",
        "#7458df","#e8875c","#b36eee","#5bdd61","#c39438","#d4c926","#dd74b6","#cf4482","#9e6c28","#86cd6f","#af511c",
        "#6759bd","#a45d4d","#5c94e5","#e28fb1","#ec2c6b","#4fd08e","#9d43ba","#7a8435","#6b699b","#7f84ea","#8d5cac",
        "#c94860","#d9a276","#a05981","#cd5644","#b3439b","#4569b1","#d9b63a","#dc3238"];
    let colors = {};
    for (let i=0; i<data.length; i++) {colors[data[i][0]] = colors_all[i]}
    if (ext.colors === 'states') {
        colors = {
            actual: '#ff7f0e',
            running: '#1f77b4',
            finished: '#2ca02c',
            failed: '#d62728',
        }
    }
    let width = 500;
    let height = 300;
    if (ext.size) {
        width = ext.size[0];
        height = ext.size[1];
    }

    var plot = c3.generate({
        bindto: '#' + divid,
        data: {
            columns: data,
            type : 'donut',
            colors: colors,
        },
        donut: {
            title: title,
        },
        tooltip: {
            format: {
                value: function (value, ratio, id) {
                    return d3.format(',.3s')(value)  + ' [' + d3.format(".1%")(ratio) + ']';
                }
            }
        },
        legend: {
            position: 'right',
            show: true
        },
        size: {
            width: width,
            height: height,
        },
    });
    return plot
}



function draw_sbar(data, divid, title, ext) {
    let width = 300;
    let height = 200;
    let x = Object.keys(data[0])[0];
    let values = Object.keys(data[0])[1];
    if (ext.size) {width = ext.size[0]; height=ext.size[1]}
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
                  //text: 'Number of tests',
                  position: 'outer-middle'
                }
            }
        },
        size: {
            width: width,
            height: height,
        },

    });
    return chart
}