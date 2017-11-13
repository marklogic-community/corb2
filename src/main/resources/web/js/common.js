"use strict";

function pad(n, z) {
    z = z || 2;
    return ("00" + n).slice(-z);
}

function isNumeric(value) {
    return !isNaN(value - parseFloat(value));
}

function msToTime(s) {
    if (!Number.isNaN(s) && s > 0) {
        var ms = s % 1000;
        s = (s - ms) / 1000;
        var secs = s % 60;
        s = (s - secs) / 60;
        var mins = s % 60;
        var hrs = (s - mins) / 60;

        return pad(hrs) + ":" + pad(mins) + ":" + pad(secs) + "." + ms;
    } else {
        return "";
    }
}

function commandActionParameter(job) {
    var commandParam = "command=";
    if (job.paused) {
        commandParam += "resume";
    } else {
        commandParam += "pause";
    }
    return commandParam;
}
