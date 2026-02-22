"use strict";

// ----------------------
// Shared utilities
// ----------------------
export function msToTime(ms) {
    if (!Number.isFinite(ms) || ms < 0) { return "00:00:00"; }
    const totalSeconds = Math.floor(ms / 1000);
    const h = String(Math.floor(totalSeconds / 3600)).padStart(2, "0");
    const m = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, "0");
    const s = String(totalSeconds % 60).padStart(2, "0");
    return `${h}:${m}:${s}`;
}

function withTimeout(ms = 8000) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), ms);
    return { signal: controller.signal, done: () => clearTimeout(t) };
}

function jobBaseUrl(job) {
    // job: { id, host, port }
    const origin = `${location.protocol}//${job.host}${job.port ? `:${job.port}` : ""}`;
    return new URL(`/${encodeURIComponent(job.id)}`, origin);
}

function metricsUrl(host, port, { concise = true } = {}) {
    const url = new URL(`http://${host}:${port}/`);
    url.searchParams.set("format", "json");
    if (concise) {
        // Presence-only parameter (serialized as `concise=`); server should treat presence as true
        url.searchParams.append("concise", "");
    }
    return url;
}

async function fetchJson(url, { signal } = {}) {
    try {
        const res = await fetch(url.toString(), { method: "GET", headers: { "Accept": "application/json" }, signal });
        if (!res.ok) {
            const err = new Error(`GET ${url} failed with ${res.status}`);
            err.status = res.status;
            throw err;
        }
        return await res.json();
    } catch (e) {
        // Map network/abort failures to synthetic -1 to retain legacy handling
        if (e && typeof e.status === "undefined") {
            e.status = -1;
        }
        throw e;
    }
}

// POST commands via QUERY STRING (server requires query params)
async function postQuery(job, params, { signal } = {}) {
    const url = jobBaseUrl(job);
    url.searchParams.set("format", "json");
    // append provided params as query string
    for (const [k, v] of Object.entries(params)) {
        if (typeof v === "undefined" || v === null) { continue; }
        url.searchParams.set(k, String(v));
    }
    try {
        const res = await fetch(url.toString(), { method: "POST", signal });
        if (!res.ok) {
            const err = new Error(`POST ${url} failed with ${res.status}`);
            err.status = res.status;
            throw err;
        }
        return await res.json();
    } catch (e) {
        if (e && typeof e.status === "undefined") {
            e.status = -1;
        }
        throw e;
    }
}

function isDone(job) {
    const total = job?.totalNumberOfTasks ?? 0;
    const progressed = (job?.numberOfSucceededTasks ?? 0) + (job?.numberOfFailedTasks ?? 0);
    const hasExit = Number.isInteger(job?.exitCode);
    return hasExit || (total > 0 && progressed >= total);
}

function isRunning(job) {
    return !isDone(job) && (job?.currentThreadCount ?? 0) !== 0;
}

// Minimal toast manager
function toastMixin() {
    return {
        toasts: [], // { id, message, type }
        _toastId: 0,
        pushToast(message, type = "error", duration = 4000) {
            const id = ++this._toastId;
            this.toasts.push({ id, message, type });
            setTimeout(() => this.removeToast(id), duration);
        },
        removeToast(id) {
            const idx = this.toasts.findIndex((t) => t.id === id);
            if (idx !== -1) { this.toasts.splice(idx, 1); }
        }
    };
}

// ----------------------
// Single-Job View Component
// ----------------------
export function jobStatus({ pollingMs = 3000 } = {}) {
    return {
        ...toastMixin(),
        job: { currentThreadCount: 0 },
        timers: new Map(),
        error: null,
        pending: false, // for pause/resume only; thread input remains enabled
        // First load retrieves full payload (with options), subsequent polls use concise
        _firstLoad: true,

        get totalNumberOfTasks() { return this.job?.totalNumberOfTasks ?? 0; },
        get exitCode() { return this.job.exitCode ?? 0; },
        get successPercent() {
            const ok = this.job?.numberOfSucceededTasks ?? 0;
            const fail = this.job?.numberOfFailedTasks ?? 0;
            const total = ok + fail;
            return total ? ((ok / total) * 100).toFixed(2) : "0.00";
        },
        get failedPercent() {
            const ok = this.job?.numberOfSucceededTasks ?? 0;
            const fail = this.job?.numberOfFailedTasks ?? 0;
            const total = ok + fail;
            return total ? ((fail / total) * 100).toFixed(2) : "0.00";
        },
        get jobDuration() { return msToTime(this.job.totalRunTimeInMillis); },
        isRunning(job = this.job) { return isRunning(job); },

        init() {
            this.refresh();
            const url = this.jobStatsUrl();
            const id = setInterval(() => this.refresh(), pollingMs);
            this.timers.set(url.toString(), id);
            addEventListener("beforeunload", () => this.destroy());
        },

        destroy() {
            for (const [, id] of this.timers) { clearInterval(id) };
            this.timers.clear();
        },

        jobStatsUrl() {
            const url = new URL(location.href);
            url.searchParams.set("format", "json");
            if (!this._firstLoad) {
                // Compact payload for polling cycles
                url.searchParams.append("concise", "");
            }
            return url;
        },

        async refresh() {
            const url = this.jobStatsUrl();
            const { signal, done } = withTimeout(8000);
            try {
                const data = await fetchJson(url, { signal });
                this.loadData(data);
                if (isDone(this.job)) {
                    this.stopTimer(url.toString());
                }
            } catch (e) {
                // Handle 404 or synthetic -1 (no backend)
                if (e.status === 404 || e.status === -1) {
                    this.stopTimer(url.toString());
                }
                console.warn(e);
                this.error = e.message;
                this.pushToast(`Job status error: ${e.status === -1 ? jobBaseUrl(this.job) + " unavailable" : e.message}`);
            } finally {
                this._firstLoad = false;
                done();
            }
        },

        loadData(response) {
            const responseJobs = [].concat(response.jobs ?? response);
            responseJobs.forEach(obj => {
                const j = obj?.job ?? obj;
                if (j) { this.job = Object.assign(this.job, j); }
            });
        },

        stopTimer(key) {
            const id = this.timers.get(key);
            if (id) clearInterval(id);
            this.timers.delete(key);
        },

        async togglePause(job) {
            if (this.pending) return;
            this.pending = true;
            const params = { command: job.paused ? "resume" : "pause" };
            const { signal, done } = withTimeout(8000);
            try {
                const data = await postQuery(job, params, { signal });
                this.loadData(data);
            } catch (e) {
                console.warn(e);
                this.error = e.message;
                this.pushToast(`Command failed (${e.status === -1 ? jobBaseUrl(job) + " unavailable" : e.status})`);
            } finally {
                done();
                this.pending = false;
            }
        },

        _threadTimer: null,
        updateThreadCount(job, threads) {
            const n = Number(threads);
            if (!Number.isFinite(n) || n < 1) return;
            this.job.currentThreadCount = n; // optimistic
            if (this._threadTimer) { clearTimeout(this._threadTimer); }
            this._threadTimer = setTimeout(() => this.commitThreadCount(job, n), 300);
        },

        async commitThreadCount(job, n) {
            const params = { "thread-count": n };
            const { signal, done } = withTimeout(8000);
            try {
                const data = await postQuery(job, params, { signal });
                this.loadData(data);
            } catch (e) {
                console.warn(e);
                this.error = e.message;
                this.pushToast(`Thread update failed (${e.status === -1 ? jobBaseUrl(job) + " unavailable" : e.status})`);
                // Optional: re-fetch to reconcile optimistic update
                this.refresh();
            } finally {
                done();
            }
        },

        msToTime,
    };
}

// ----------------------
// Dashboard (Multi-Job) Component
// ----------------------
export function dashboard({ pollingMs = 2000 } = {}) {
    const HOST_RE = /^(?:\[(?:[A-Fa-f0-9:]+)\]|(?:[A-Za-z0-9-.]+))$/;
    return {
        ...toastMixin(),
        monitorHosts: [], // [{ host: string, ports: string }]
        timers: new Map(), // metricsUrl -> intervalId
        jobs: [],
        error: null,
        pendingJobs: new Set(), // job.id values for which a command is in-flight

        init() {
            try {
                this.addMonitor({ host: location.hostname , ports: location.port });
            } catch {}
            addEventListener("beforeunload", () => this.destroy());
        },

        destroy() {
            for (const [, id] of this.timers) clearInterval(id);
            this.timers.clear();
        },

        withinRange(n) { return Number.isFinite(n) && n >= 1 && n <= 65535; },

        isPortRangeValid(s) {
            if (!s) { return false; }
            return s.split(",").every(token => {
                token = token.trim();
                const rangeMatch = token.match(/^(\d{1,5})-(\d{1,5})$/);
                const singleMatch = token.match(/^\d{1,5}$/);
                if (rangeMatch) {
                    const a = +rangeMatch[1], b = +rangeMatch[2];
                    return this.withinRange(a) && this.withinRange(b) && a <= b;
                }
                if (singleMatch) {
                    const n = +singleMatch[0];
                    return this.withinRange(n);
                }
                return false;
            });
        },

        parseExternalHostAndPorts(metricsHost) {
            const out = [];
            let h = String(metricsHost.host || "").trim();
            if (!h) { return out; }
            try {
                if (h.startsWith("http://") || h.startsWith("https://")) {
                    const u = new URL(h);
                    h = u.hostname;
                } else if (!HOST_RE.test(h)) {
                    return out; // invalid host token
                }
            } catch {
                return out;
            }

            const ports = String(metricsHost.ports || "").trim();
            if (!ports || !this.isPortRangeValid(ports)) { return out; }

            for (const token of ports.split(",")) {
                const t = token.trim();
                if (/^\d{1,5}-\d{1,5}$/.test(t)) {
                    const [a, b] = t.split("-").map(Number);
                    for (let p = a; p <= b; p++) { out.push([h, p]) };
                } else {
                    out.push([h, Number(t)]);
                }
            }
            return out;
        },

        addMonitor(monitorHost) {
            const items = this.parseExternalHostAndPorts(monitorHost);
            if (items.length === 0) { return; }
            for (const [h, p] of items) { this.scheduleMetricsRefresh(h, p); }
            this.monitorHosts.push(JSON.parse(JSON.stringify(monitorHost)));
            monitorHost.host = "";
            monitorHost.ports = "";
        },

        removeMonitor(i) {
            const parsed = this.parseExternalHostAndPorts(this.monitorHosts[i] || {});
            for (const [h, p] of parsed) this.stopTimer(metricsUrl(h, p, { concise: true }).toString());
            this.monitorHosts.splice(i, 1);
        },

        async metricsRefresh(url) {
            const { signal, done } = withTimeout(8000);
            try {
                const data = await fetchJson(url, { signal });
                this.loadData(data);
            } catch (e) {
                if (e.status === 404 || e.status === -1) {
                    this.stopTimer(url.toString());
                }
                console.warn(e);
                this.error = e.message;
                this.pushToast(`Metrics error: ${e.status === -1 ? url + " unavailable" : e.message}`);
            } finally {
                done();
            }
        },

        scheduleMetricsRefresh(host, port) {
            const url = metricsUrl(host, port, { concise: true });
            this.metricsRefresh(url);
            const id = setInterval(() => this.metricsRefresh(url), pollingMs);
            this.timers.set(url.toString(), id);
        },

        stopTimer(key) {
            const id = this.timers.get(key);
            if (id) { clearInterval(id); }
            this.timers.delete(key);
        },

        loadData(response) {
            const responseJobs = [].concat(response.jobs ?? response);
            responseJobs.forEach((obj) => {
                const j = obj?.job ?? obj;
                if (!j?.id) return;
                const idx = this.jobs.findIndex(x => x.id === j.id);
                if (idx !== -1) this.jobs.splice(idx, 1, j);
                else this.jobs.push(j);
            });
        },

        isRunning,
        msToTime,

        isPending(id) { return this.pendingJobs.has(id); },

        async togglePause(job) {
            if (this.isPending(job.id)) { return };
            this.pendingJobs.add(job.id);
            const params = { command: job.paused ? "resume" : "pause" };
            const { signal, done } = withTimeout(8000);
            try {
                const data = await postQuery(job, params, { signal });
                this.loadData(data);
            } catch (e) {
                console.warn(e);
                this.error = e.message;
                this.pushToast(`Command failed (${e.status === -1 ? jobBaseUrl(job) + " unavailable" : e.status})`);
            } finally {
                done();
                this.pendingJobs.delete(job.id);
            }
        },

        _threadTimers: new Map(),
        updateThreadCount(job, threads) {
            const n = Number(threads);
            if (!Number.isFinite(n) || n < 1) { return };
            job.currentThreadCount = n; // optimistic
            const existing = this._threadTimers.get(job.id);
            if (existing) clearTimeout(existing);
            const to = setTimeout(() => this.commitThreadCount(job, n), 300);
            this._threadTimers.set(job.id, to);
        },

        async commitThreadCount(job, n) {
            const params = { "thread-count": n };
            const { signal, done } = withTimeout(8000);
            try {
                const data = await postQuery(job, params, { signal });
                this.loadData(data);
            } catch (e) {
                console.warn(e);
                this.error = e.message;
                this.pushToast(`Thread update failed (${e.status === -1 ? jobBaseUrl(job) + " unavailable" : e.status})`);
                const url = metricsUrl(job.host, job.port, { concise: true });
                this.metricsRefresh(url);
            } finally {
                done();
            }
        },

        openJob(job) {
            const url = jobBaseUrl(job);
            window.open(url.toString(), "_blank");
        }
    };
}
