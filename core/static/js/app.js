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



