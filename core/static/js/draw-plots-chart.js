
function getWidth() {
  return Math.min(
    document.body.scrollWidth,
    document.documentElement.scrollWidth,
    document.body.offsetWidth,
    document.documentElement.offsetWidth,
    document.documentElement.clientWidth
  );
}

var colors = [
        "#62c9ae","#52cad7","#d5a9e4","#e38924","#9bd438","#438760","#ca46ce","#e08284","#4ba930",
        "#a191d6","#57a3cf","#476be2","#85713b","#e35625","#a5be48","#a0c284","#498635","#e135ac","#d6c175","#dc82e1",
        "#7458df","#e8875c","#b36eee","#5bdd61","#c39438","#d4c926","#dd74b6","#cf4482","#9e6c28","#86cd6f","#af511c",
        "#6759bd","#a45d4d","#5c94e5","#e28fb1","#ec2c6b","#4fd08e","#9d43ba","#7a8435","#6b699b","#7f84ea","#8d5cac",
        "#c94860","#d9a276","#a05981","#cd5644","#b3439b","#4569b1","#d9b63a","#dc3238"];

function prepare_scatter_chart(datasets, options, annotations) {
  var timeFormat = 'YYYY-MM-DD HH:mm:ss';
  var config = {
    type: 'scatter',
    data: {
      datasets: datasets,
    },
    options: {
      scales: {
        x: {
          type: 'time',
          time: {
            parser: timeFormat,
            displayFormats: {
              hour: 'll hA'
            }
          },
          suggestedMin: options.xmin,
          suggestedMax: options.xmax,
          title: {
            display: true,
            text: 'Time, UTC'
          },
          ticks: {
            source: 'auto'
          }
        },
        y: {
          title: {
            display: true,
            text: options.ylabel,
          },
          suggestedMax: options.ymax,
        },
      },
      plugins: {
        legend: {
          display: true,
          labels: {
            usePointStyle: true,
          }
        },
        tooltip: {
          enabled: true,
          mode: 'point',
          yAlign: 'top',
          position: 'nearest',
          caretPadding: 5,
          callbacks: {
            title: (tooltipItems) => {
                var title = tooltipItems[0].raw.x  || '';
                if (title) {
                    title = 'Time: ' + title + ' UTC';
                }
                return title;
            },
            label: (tooltipItems) => {
              let dslabel = '';
              if (tooltipItems.constructor === Array ) {
                tooltipItems.forEach((tooltipItem) => {
                  dslabel += ' ' + tooltipItem.dataset.label + ', pandaid: ' + tooltipItem.raw.label;
                });
              }
              else {
                dslabel = ' ' + tooltipItems.dataset.label + ', pandaid: ' + tooltipItems.raw.label;
              }
              return dslabel;
            },
          },
        },
      },
      layout: {
        padding: {
          left: 0,
          right: 20,
          top: 0,
          bottom: 80
        }
      },
      events: ['click', 'mousemove'],
      animation: {
        duration: 0 // general animation time
      },
      hover: {
        // intersect: false,
        animationDuration: 0 // duration of animations when hovering an item
      },
      responsiveAnimationDuration: 0, // animation duration after a resize
      responsive: false,
    }
  };

  if (annotations) {
    config.options.plugins.annotation = {
      annotations: annotations,
    }
  }
  return config

}


function prepare_stacked_histogram_chart(rawdata, options) {
  let labels = rawdata[0];
  labels.shift();
  var data = {
    labels: labels,
    datasets: [],
  };
  rawdata.shift();
  rawdata.forEach((val, i) => {
    let label = val[0];
    let row = val;
    row.shift();
    data.datasets.push({
      label: label,
      backgroundColor: colors[i],
      data: row,
    })
  });


  var config = {
    type: 'bar',
    data: data,
    options: {
      scales: {
        xAxes: [{
          stacked: true,
          scaleLabel: {
            display: true,
            labelString: options.xlabel,
          },
          ticks: {
            source: 'auto'
          }
        }],
        yAxes: [{
          stacked: true,
          scaleLabel: {
            display: true,
            labelString: options.ylabel,
          },
        }],
      },
      legend: {
        display: true,
        position: 'bottom',
        labels: {
          usePointStyle: true,
        }
      },
      layout: {
        padding: {
          left: 0,
          right: 20,
          top: 0,
          bottom: 20
        }
      },
      events: ['click', 'mousemove'],
      animation: {
        duration: 0 // general animation time
      },
      hover: {
        // intersect: false,
        animationDuration: 0 // duration of animations when hovering an item
      },
      responsiveAnimationDuration: 0, // animation duration after a resize
      responsive: false,
    }
  };

  return config

}