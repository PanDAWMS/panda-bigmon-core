
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
            labelString: 'Number of successfully finished jobs'
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
          bottom: 0
        }
      },
      events: ['click'],
      animation: {
        duration: 0 // general animation time
      },
      hover: {
        animationDuration: 0 // duration of animations when hovering an item
      },
      responsiveAnimationDuration: 0, // animation duration after a resize
      tooltips: {
        enabled: false,
      },
    }
  };

  return config

}