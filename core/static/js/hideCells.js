/**
 * Created by tkorchug on 22.03.18.
 *
 * Function to hide cells in table with specific class name, created for errors scattering matrix pages
 *
 */

function toggleLowStatsCells(classname, tableid, buttonid) {
    var classesOfTableCells = ['sm_alarm_fill', 'sm_alarm_light_fill', 'sm_warning_fill', 'sm_warning_light_fill', 'sm_ok_fill', 'sm_ok_light_fill'];
    if ($("." + classname).is(':visible')) {
        // hide text inside a tag having classname assigned//
        $("." + classname).hide();
        // remove color filling of td contained classname by adding a '_hidden' tail to class name//
        $("." + classname).each(function () {
            var classOfElement = $(this).parent().attr('class');
            if (classOfElement.length > 0 && (jQuery.inArray(classOfElement,classesOfTableCells) >= 0)) {
                $(this).parent().removeClass(classOfElement);
                classOfElement += '_hidden';
                $(this).parent().addClass(classOfElement);
            }
        });
         // go through rows and hide it if all child cells are hidden or empty
        $('#' + tableid + ' > tbody').children('tr').each(function () {
            var isEmpty = true;
            var parent = $(this);
            parent.children('td').each(function () {
                var attr = $(this).attr('class');
                if ((typeof attr !== typeof undefined && attr !== false) && (!($(this).attr('class').indexOf('_hidden'))) || ($(this).children('a').length > 0 && $(this).children(':first').is(':visible'))) {
                    isEmpty = false;
                }
            });
            if (isEmpty) {parent.hide()}
        });
        document.getElementById(buttonid).innerHTML = 'Show low statistics cells';
    }
    else {
        // show all rows //
        $('#' + tableid + ' > tbody').children('tr').each(function () {
            var parent = $(this);
            parent.show();
        });
        //recover color classes for cells i.e. remove '_hidden' tail from class names//
        $("." + classname).each(function () {
            var classOfElement = $(this).parent().attr('class');
            $(this).parent().removeClass(classOfElement);
            if (classOfElement.length > 0 && classOfElement.indexOf('_hidden')) {
                classOfElement = classOfElement.replace('_hidden', '');
                $(this).parent().addClass(classOfElement);
            }
        });
        $("." + classname).show();
        document.getElementById(buttonid).innerHTML = 'Hide low statistics cells';
    }
}