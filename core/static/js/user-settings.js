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


