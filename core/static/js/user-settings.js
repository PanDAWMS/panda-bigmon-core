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


function disableDetails(tablename) {
    var basename = tablename.replace(/ *?summary/g,'');
    var inputid = 'switch-table-' + tablename;
    var input = document.getElementById(inputid);
    var switchmenuid = basename + '-menu';
    var switchname = 'switch-' + basename;
    var switches = document.getElementsByName(switchname);
    if (!input.checked) {
        for (var sw = 0; sw<switches.length; sw++ ) {
            if (switches[sw].hasAttribute('checked')) {
                switches[sw].removeAttribute('checked');
            }
        }
        document.getElementById(switchmenuid).setAttribute('aria-expanded', 'false');
        document.getElementById(switchmenuid).classList.add('invisible');
    }
    else {
        document.getElementById(switchmenuid).classList.remove('invisible');
        // var userSettings = {{ userPreferences.jobattr | safe }} ;
        // for (var i = 0; i<switches.length; i++ ) {
        //     if (userSettings.indexOf(switches[i].id.split('-')[2]) != -1) {
        //         switches[i].addAttribute('checked');
        //     }
        // }
    }
}
