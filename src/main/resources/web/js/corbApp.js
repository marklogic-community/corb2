"use strict";
const metricsPath = "?format=json";
const host = location.hostname || "localhost";
const port = location.port;


/**
 * Utility: format milliseconds to HH:mm:ss
 */
export function msToTime(ms) {
    if (!Number.isFinite(ms) || ms < 0) return "00:00:00";
    const totalSeconds = Math.floor(ms / 1000);
    const h = String(Math.floor(totalSeconds / 3600)).padStart(2, "0");
    const m = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, "0");
    const s = String(totalSeconds % 60).padStart(2, "0");
    return `${h}:${m}:${s}`;
}

/**
 * Utility: return URL for Job
 */
function toUrl(job) {
    return `http://${job.host}:${job.port}/${job.id}`;
}

// If a job doesn't exist anymore, or the server cannot be reached - stop checking
function handleError(response) {
    if (response.status === "404" || response.status === -1) {
        clearInterval(this.promises[response.url]);
    }
}

function commandActionParameter(job) {
    const action = job.paused ? "resume" : "pause";
    return `command=${action}`;
}

async function togglePause(job) {
    await fetch(toUrl(job) + metricsPath + "&" + commandActionParameter(job), {method: "POST"})
        .then(async (response) => {
            if (!response.ok) {
                return handleError(response);
            }
            await response.json().then((response) => this.loadData(response));
        })
}

async function updateThreadCount(job, threads) {
    job.currentThreadCount = threads;
    await fetch(toUrl(job) + metricsPath + "&thread-count=" + threads, {method: "POST"})
        .then(async response => {
            if (!response.ok) {
                return handleError(response);
            }
            await response.json().then((response) => this.loadData(response));
        }).catch((error) => console.log("Failed to update thread count for " + job.id));
}
