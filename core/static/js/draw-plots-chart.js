
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
        "#b22222", "#62c9ae","#d5a9e4","#52cad7","#e38924","#9bd438","#438760","#ca46ce","#e08284","#4ba930",
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
        zoom: {
          pan: {
            enabled: true,
            mode: 'x',
            scaleMode: 'x',
            threshold: 50,
          },
          zoom: {
            drag: {
              enabled: true,
              mode: 'x',
              threshold: 20,
              modifierKey: 'shift',
            },
            pinch: {
              enabled: true,
            },
            mode: 'x',
          }
        }
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


function prepare_stacked_timeseries_chart(rawdata, options) {
  let labels = rawdata[0];
  labels.shift();
  var data = {
    labels: labels,
    datasets: [],
  };
  var datasets = {};
  var color_schema = {};
  if ("color_schema" in options) {
    color_schema = options.color_schema;
  }
  else {
    color_schema = labels.reduce((acc, label, index) => {acc[label] = colors[index]; return acc; }, {});
  }
  labels.forEach((label) => {
    datasets[label] = {label: label, backgroundColor: color_schema[label], fill:true,  data: [] };
  });
  rawdata.shift(); // remove header
  rawdata.forEach((row) => {
    labels.forEach((label, i) => {
      datasets[label].data.push({
        x: row[0],
        y: row[i+1],
        label: label,
      })
    });
  });
  data.datasets = Object.values(datasets);

  var config = {
    type: 'bar',
    data: data,
    options: {
      plugins: {
        title: {
          display: true,
          text: options.title,
        },
        legend: {
          display: true,
          position: 'bottom',
        },
      },
      barPercentage: 0.9,      // Max width for each bar within a category
      categoryPercentage: 0.9,   // Max width for each category
      scales: {
        x: {
          type: 'time',
          time: {
            parser: "YYYY-MM-DD HH:mm:ss",
            unit: 'hour',
            displayFormats: {
              minute: 'YYYY-MM-DD HH:mm',
              hour: 'YYYY-MM-DD HH:mm'
            },
          },
          stacked: true,
          offset: true,
          title: {
              display: true,
              text: options.labels[0],
          },
          ticks: {
            // autoSkip: false,
            source: 'data'
          },
          min: rawdata[0][0],
          max: rawdata[rawdata.length-1][0],
          grid: {
            display: false  // Disable vertical grid lines
          },
        },
        y: {
          stacked: true,
          beginAtZero: true,
          title: {
              display: true,
              text: options.labels[1]
          }
        }
      },
      layout: {
        padding: {
          left: 0,
          right: 0,
          top: 0,
          bottom: 0
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
      maintainAspectRatio: false,
    }
  };

  return config

}


function prepare_pie_chart(raw_data, options) {
  const data = {
    labels: [],
    datasets: [
      {
        label: '',
        data: [],
        backgroundColor: [],
      }
    ]
  };
  const color_schema = {};
  Object.keys(raw_data).forEach((key, index) => {
    data.labels.push(key);
    data.datasets[0].data.push(raw_data[key]);
    data.datasets[0].backgroundColor.push((key !== 'Other') ? colors[index] : "#d3d3d3");
  });

  var config = {
    type: 'doughnut',
    data: data,
    options: {
      plugins: {
        legend: {
          position: 'bottom',
        },
        title: {
          display: true,
          text: options.title,
        }
      },
      animation: {
        duration: 0 // general animation time
      },
      hover: {
        // intersect: false,
        animationDuration: 0 // duration of animations when hovering an item
      },
      responsiveAnimationDuration: 0, // animation duration after a resize
      responsive: false,
      maintainAspectRatio: false,
      layout: {
        padding: {
          left: 0,
          right: 0,
          top: 0,
          bottom: 0
        }
      },
    },
  }


  return config
}
