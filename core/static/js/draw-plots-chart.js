
function getWidth() {
  return Math.min(
    document.body.scrollWidth,
    document.documentElement.scrollWidth,
    document.body.offsetWidth,
    document.documentElement.offsetWidth,
    document.documentElement.clientWidth
  );
}

function prepare_scatter_chart(datasets, options) {

  var timeFormat = 'YYYY-MM-DD HH:mm:ss';
  var config = {
    type: 'scatter',
    data: {
      datasets: datasets,
    },
    options: {
      scales: {
        xAxes: [{
          type: 'time',
          time: {
            parser: timeFormat,
            displayFormats: {
              hour: 'll hA'
            }
          },
          scaleLabel: {
            display: true,
            labelString: 'Time, UTC'
          },
          ticks: {
            source: 'auto'
          }
        }],
        yAxes: [{
          scaleLabel: {
            display: true,
            labelString: options.ylabel,
          },
        }],
      },
      legend: {
        display: true,
        labels: {
          usePointStyle: true,
        }
      },
      layout: {
        padding: {
          left: 0,
          right: 20,
          top: 0,
          bottom: 100
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
      tooltips: {
        enabled: true,
        mode: 'point',
        yAlign: 'top',
        position: 'nearest',
        caretPadding: 5,
        callbacks: {
          title: function(tooltipItem, data) {
              var title = tooltipItem[0].label  || '';

              if (title) {
                  title = 'Time: ' + title + ' UTC';
              }
              return title;
          },
          label: function(tooltipItem, data) {
              var label = data.datasets[tooltipItem.datasetIndex].data[tooltipItem.index].label || '';
              var dslabel = data.datasets[tooltipItem.datasetIndex].label || '';
              if (label && dslabel) {
                  dslabel += ', pandaid: ' + label;
              }
              return dslabel;
          },
        },
      },
    }
  };

  return config

}