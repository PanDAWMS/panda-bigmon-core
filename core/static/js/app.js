$(document).foundation();

$(function(){
    $('#time-window-input-date-from').fdatepicker({
        format: 'yyyy-mm-dd hh:ii',
        disableDblClickSelection: true,
        language: 'en',
        pickTime: true
    });
    $('#time-window-input-date-to').fdatepicker({
        format: 'yyyy-mm-dd hh:ii',
        disableDblClickSelection: true,
        language: 'en',
        pickTime: true
    });
});

function diff_hours(dt2, dt1) {
    let diff =(dt2.getTime() - dt1.getTime()) / 1000;
    diff /= (60 * 60);
    return Math.abs(Math.round(diff));
}

function subtract_hours(dt, h) {
  let dt_new = new Date(dt);
  dt_new.setTime(dt_new.getTime() - (h*60*60*1000));
  return dt_new;
}

function disable_input(item, options) {
    let available_options = options.slice();
    let item_index = available_options.indexOf(item);
    available_options.splice(item_index, 1);
    available_options.forEach(function (val) {
        let els = document.querySelectorAll('input.time-window-input-' + val);
        for (let i = 0; i < els.length; i++) { els[i].disabled = true; }
    });
    let els = document.querySelectorAll('input.time-window-input-' + item);
    for (let i = 0; i < els.length; i++) { els[i].disabled = false; }
}

(function ($) {
  $.fn.goTo = function () {
      $('html, body').animate({
          scrollTop: $(this).offset().top + 'px'
      }, 'fast');
      return this;
  }
})(jQuery);

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

function getNCharsShorten() {
  let nChars = 1000;
  let width = getWidth();
  if (width >= 1440) {
      nChars = 4000;
  }
  else if (width >= 1024) {
      nChars = 2500;
  }
  return nChars
}