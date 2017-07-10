/**
 * Created by tkorchug on 21.06.17.
 */

function getSettings(name) {
    var settingsStr = '';
    var settings = document.getElementsByName(name);
    for (var i = 0; i < settings.length; i++) {
        var element = settings[i];
        if (element.checked) {
            settingsStr += element.id.split("-")[2];
            settingsStr += ','
        }
    }
    if (settingsStr[settingsStr.length - 1] == ',') {
        settingsStr = settingsStr.substring(0,settingsStr.length - 1);
    }
    return settingsStr;
}
// function saveSettings() {
//     var settingTables = getSettings('switch-table');
//     var settingJobAttr = getSettings('switch-jobsattr');
//     var url = window.location.href;
//     if (settingTables.length > 1) {
//         url += '&tables=' + settingTables;
//     }
//     if (settingJobAttr.length > 1) {
//         url += '&jobattr=' + settingJobAttr;
//     }
//     window.location = url;
//
//     $.ajax({
//         url: {% url 'savesettings' %},
//         data: nosorturl.replace('/tasks/?',''),
//         async: true
//     })
//     .done(function (response) {
//         $('#div-sum').html(response);
//     });
//
//     // var usersettingsForm = document.forms['form-usersettings'];
//     // usersettingsForm.elements["tables"].value = settingTables;
//     // usersettingsForm.elements["jobattr"].value = settingJobAttr;
//     //
//     // document.getElementById("form-usersettings").submit();
// }

function disableDetails(inputid,name,menuid,menuitemid) {
    var input = document.getElementById(inputid);
    var switches = document.getElementsByName(name);
    if (!input.checked) {
        for (var sw = 0; sw<switches.length; sw++ ) {
            if (switches[sw].hasAttribute('checked')) {
                switches[sw].removeAttribute('checked');
            }
        }
        document.getElementById(menuid).setAttribute('aria-expanded', 'false');
        document.getElementById(menuid).classList.add('invisible');
    }
    else {
        document.getElementById(menuid).classList.remove('invisible');
        // var userSettings = {{ userPreferences.jobattr | safe }} ;
        // for (var i = 0; i<switches.length; i++ ) {
        //     if (userSettings.indexOf(switches[i].id.split('-')[2]) != -1) {
        //         switches[i].addAttribute('checked');
        //     }
        // }
    }
}