
function ellipsisCustom(d, type, cutoff, wordbreak, escapeHtml, customTooltip, tooltipDirection) {
    var esc = function (t) {
        return ('' + t)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    };

    // Order, search and type get the original data
    if (type !== 'display') {
        return d;
    }
    if (typeof d !== 'number' && typeof d !== 'string') {
        if (escapeHtml) {
            return esc(d);
        }
        return d;
    }
    d = d.toString(); // cast numbers
    if (d.length <= cutoff) {
        if (escapeHtml) {
            return esc(d);
        }
        return d;
    }
    var shortened = d.substr(0, cutoff - 1);
    // Find the last white space character in the string
    if (wordbreak) {
        shortened = shortened.replace(/\s([^\s]*)$/, '');
    }
    // Protect against uncontrolled HTML input
    if (escapeHtml) {
        shortened = esc(shortened);
    }
    var output = ''
    if (customTooltip) {
      output = '<div class="bp-tooltip ' + tooltipDirection + '">' + shortened + '&#8230;<span class="tooltip-text">' + esc(d) + '</span></div>'
    }
    else {
      output = '<span class="ellipsis" title="' + esc(d) + '">' + shortened + '&#8230;</span>'
    }
    return output;
}

