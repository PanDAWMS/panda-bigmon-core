// Foundation JavaScript
// Documentation can be found at: http://foundation.zurb.com/docs

$(document).foundation();

$(function(){
    $('.fdatetimepicker').fdatepicker({
        format: 'yyyy-mm-dd hh:ii:ss',
        disableDblClickSelection: true,
        language: 'vi',
        pickTime: true
    });
});


