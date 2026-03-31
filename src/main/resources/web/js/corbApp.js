"use strict";

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
    const origin = `${location.protocol}//${job.host}${job.port ? `:${job.port}` : ""}`;
    return new URL(`/${encodeURIComponent(job.id)}`, origin);
}

function metricsUrl(host, port, { concise = true } = {}) {
    const url = new URL(`http://${host}:${port}/`);
    url.searchParams.set("format", "json");
    if (concise) {
        url.searchParams.append("concise", "");
    }
    return url;
}

function builderUrl(path) {
    return new URL(path, location.origin);
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
        if (e && typeof e.status === "undefined") {
            e.status = -1;
        }
        throw e;
    }
}

async function postQuery(job, params, { signal } = {}) {
    const url = jobBaseUrl(job);
    url.searchParams.set("format", "json");
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

async function postForm(url, params, { signal } = {}) {
    try {
        const res = await fetch(url.toString(), {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
            body: params.toString(),
            signal,
        });
        if (!res.ok) {
            const message = await res.text();
            const err = new Error(message || `POST ${url} failed with ${res.status}`);
            err.status = res.status;
            throw err;
        }
        return res;
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

function toastMixin() {
    return {
        toasts: [],
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

function parseContentDispositionFilename(headerValue) {
    if (!headerValue) {
        return null;
    }
    const match = headerValue.match(/filename="?([^";]+)"?/i);
    return match ? match[1] : null;
}

function escapePropertiesValue(value) {
    if (value === null || typeof value === "undefined") {
        return "";
    }
    let text = String(value)
        .replace(/\\/g, "\\\\")
        .replace(/\n/g, "\\n")
        .replace(/\r/g, "\\r")
        .replace(/\t/g, "\\t");
    if (text.startsWith(" ")) {
        text = `\\${text}`;
    }
    return text;
}

const CONNECTION_COMPONENT_OPTIONS = new Set([
    "XCC-PROTOCOL",
    "XCC-HOSTNAME",
    "XCC-PORT",
    "XCC-DBNAME",
    "XCC-USERNAME",
    "XCC-PASSWORD",
    "XCC-BASE-PATH",
    "XCC-API-KEY",
    "XCC-OAUTH-TOKEN",
    "XCC-GRANT-TYPE",
    "XCC-TOKEN-DURATION",
    "XCC-TOKEN-ENDPOINT"
]);

export function jobStatus({ pollingMs = 3000 } = {}) {
    return {
        ...toastMixin(),
        job: { currentThreadCount: 0 },
        timers: new Map(),
        error: null,
        pending: false,
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
            for (const [, id] of this.timers) { clearInterval(id); }
            this.timers.clear();
        },

        jobStatsUrl() {
            const url = new URL(location.href);
            url.searchParams.set("format", "json");
            if (!this._firstLoad) {
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
            responseJobs.forEach((obj) => {
                const j = obj?.job ?? obj;
                if (j) { this.job = Object.assign(this.job, j); }
            });
        },

        stopTimer(key) {
            const id = this.timers.get(key);
            if (id) { clearInterval(id); }
            this.timers.delete(key);
        },

        async togglePause(job) {
            if (this.pending) { return; }
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
            if (!Number.isFinite(n) || n < 1) { return; }
            this.job.currentThreadCount = n;
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
                await this.refresh();
            } finally {
                done();
            }
        },

        msToTime,
    };
}

export function dashboard({ pollingMs = 2000 } = {}) {
    const HOST_RE = /^(?:\[(?:[A-Fa-f0-9:]+)\]|(?:[A-Za-z0-9-.]+))$/;
    return {
        ...toastMixin(),
        monitorHosts: [],
        timers: new Map(),
        jobs: [],
        error: null,
        pendingJobs: new Set(),
        currentTab: 1,
        builder: {
            groups: [],
            values: {},
            expandedDescriptions: {},
            additionalProperties: "",
            connectionMode: "uri",
            activeGroupId: "connection",
            searchTerm: "",
            collapsed: false,
            loadingMetadata: false,
            running: false,
            downloading: false,
            error: null,
            downloadFileName: "corb-job.properties",
        },

        init() {
            try {
                this.addMonitor({ host: location.hostname, ports: location.port });
            } catch {}
            this.loadBuilderMetadata();
            addEventListener("beforeunload", () => this.destroy());
        },

        destroy() {
            for (const [, id] of this.timers) { clearInterval(id); }
            this.timers.clear();
        },

        withinRange(n) { return Number.isFinite(n) && n >= 1 && n <= 65535; },

        isPortRangeValid(s) {
            if (!s) { return false; }
            return s.split(",").every((token) => {
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
                    return out;
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
                    for (let p = a; p <= b; p++) { out.push([h, p]); }
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
            for (const [h, p] of parsed) { this.stopTimer(metricsUrl(h, p, { concise: true }).toString()); }
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
                if (!j?.id) { return; }
                const idx = this.jobs.findIndex((x) => x.id === j.id);
                if (idx !== -1) {
                    this.jobs.splice(idx, 1, j);
                } else {
                    this.jobs.push(j);
                }
            });
        },

        isRunning,
        msToTime,

        isPending(id) { return this.pendingJobs.has(id); },

        async togglePause(job) {
            if (this.isPending(job.id)) { return; }
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
            if (!Number.isFinite(n) || n < 1) { return; }
            job.currentThreadCount = n;
            const existing = this._threadTimers.get(job.id);
            if (existing) { clearTimeout(existing); }
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
                await this.metricsRefresh(url);
            } finally {
                done();
            }
        },

        openJob(job) {
            const url = jobBaseUrl(job);
            window.open(url.toString(), "_blank");
        },

        async loadBuilderMetadata() {
            this.builder.loadingMetadata = true;
            this.builder.error = null;
            const { signal, done } = withTimeout(15000);
            try {
                const data = await fetchJson(builderUrl("/builder/metadata"), { signal });
                this.builder.groups = [].concat(data.groups ?? []);
                this.builder.activeGroupId = this.builder.groups.length ? this.builder.groups[0].id : "connection";
                const values = {};
                this.builder.groups.forEach((group) => {
                    group.options.forEach((option) => {
                        if (!Object.prototype.hasOwnProperty.call(values, option.name)) {
                            values[option.name] = "";
                        }
                    });
                });
                this.builder.values = Object.assign(values, this.builder.values);
                this.builder.expandedDescriptions = Object.assign({}, this.builder.expandedDescriptions);
            } catch (e) {
                console.warn(e);
                this.builder.error = e.message;
                this.pushToast(`Options builder error: ${e.message}`);
            } finally {
                this.builder.loadingMetadata = false;
                done();
            }
        },

        builderFieldId(name) {
            return `builder-${String(name).toLowerCase().replace(/[^a-z0-9]+/g, "-")}`;
        },

        builderDescriptionId(name) {
            return `${this.builderFieldId(name)}-description`;
        },

        setConnectionMode(mode) {
            this.builder.connectionMode = mode;
        },

        toggleBuilderCollapsed() {
            this.builder.collapsed = !this.builder.collapsed;
        },

        visibleBuilderOptions(group) {
            return (group?.options ?? []).filter((option) => this.shouldShowBuilderOption(option) && this.matchesBuilderSearch(option));
        },

        hasBuilderDescription(option) {
            return String(option?.description ?? "").trim() !== "";
        },

        toggleBuilderDescription(name) {
            const current = !!this.builder.expandedDescriptions[name];
            this.builder.expandedDescriptions = Object.assign({}, this.builder.expandedDescriptions, { [name]: !current });
        },

        isBuilderDescriptionVisible(name) {
            return !!this.builder.expandedDescriptions[name];
        },

        visibleBuilderSubgroups(group) {
            return (group?.subgroups ?? []).filter((subgroup) => this.visibleBuilderOptionsForSubgroup(group, subgroup).length > 0);
        },

        visibleBuilderOptionsForSubgroup(group, subgroup) {
            return this.visibleBuilderOptions(group).filter((option) => option.subgroupId === subgroup.id);
        },

        matchesBuilderSearch(option) {
            const term = this.builder.searchTerm.trim().toLowerCase();
            if (!term) {
                return true;
            }
            return [option.name, option.label, option.description]
                .some((value) => String(value ?? "").toLowerCase().includes(term));
        },

        shouldShowBuilderOption(option) {
            if (option.name === "XCC-CONNECTION-URI") {
                return this.builder.connectionMode === "uri";
            }
            if (CONNECTION_COMPONENT_OPTIONS.has(option.name)) {
                return this.builder.connectionMode === "fields";
            }
            return true;
        },

        builderGroupOptionCount(group) {
            return this.visibleBuilderOptions(group).length;
        },

        builderTotalOptionCount() {
            return this.builder.groups.reduce((total, group) => total + ((group?.options ?? []).length), 0);
        },

        builderVisibleOptionCount() {
            return this.builder.groups.reduce((total, group) => total + this.visibleBuilderOptions(group).length, 0);
        },

        serializeBuilderPayload(includeDownloadFileName = true) {
            const params = new URLSearchParams();
            this.builder.groups.forEach((group) => {
                (group.options ?? []).forEach((option) => {
                    if (!this.shouldShowBuilderOption(option)) {
                        return;
                    }
                    const value = this.builder.values[option.name];
                    if (value === null || typeof value === "undefined") {
                        return;
                    }
                    const normalized = String(value).trim();
                    if (normalized === "") {
                        return;
                    }
                    params.set(option.name, normalized);
                });
            });
            if (this.builder.additionalProperties.trim() !== "") {
                params.set("builder.additionalProperties", this.builder.additionalProperties);
            }
            if (includeDownloadFileName && this.builder.downloadFileName.trim() !== "") {
                params.set("builder.downloadFileName", this.builder.downloadFileName.trim());
            }
            return params;
        },

        builderPropertiesPreview() {
            const lines = [];
            for (const [name, value] of this.serializeBuilderPayload(false).entries()) {
                if (name.startsWith("builder.")) {
                    continue;
                }
                lines.push(`${name}=${escapePropertiesValue(value)}`);
            }
            if (this.builder.additionalProperties.trim() !== "") {
                if (lines.length) {
                    lines.push("");
                }
                lines.push(this.builder.additionalProperties.trim());
            }
            return lines.join("\n");
        },

        async downloadBuilderProperties() {
            this.builder.downloading = true;
            const { signal, done } = withTimeout(15000);
            try {
                const response = await postForm(builderUrl("/builder/properties"), this.serializeBuilderPayload(true), { signal });
                const blob = await response.blob();
                const contentDisposition = response.headers.get("Content-Disposition");
                const filename = parseContentDispositionFilename(contentDisposition) || this.builder.downloadFileName || "corb-job.properties";
                const blobUrl = URL.createObjectURL(blob);
                const link = document.createElement("a");
                link.href = blobUrl;
                link.download = filename;
                document.body.appendChild(link);
                link.click();
                link.remove();
                URL.revokeObjectURL(blobUrl);
                this.pushToast(`Downloaded ${filename}`, "success");
            } catch (e) {
                console.warn(e);
                this.builder.error = e.message;
                this.pushToast(`Download failed: ${e.message}`);
            } finally {
                this.builder.downloading = false;
                done();
            }
        },

        async runBuilderJob() {
            this.builder.running = true;
            const { signal, done } = withTimeout(15000);
            try {
                const response = await postForm(builderUrl("/builder/jobs"), this.serializeBuilderPayload(false), { signal });
                const data = await response.json();
                await this.metricsRefresh(metricsUrl(location.hostname, location.port, { concise: true }));
                this.pushToast(`Started job ${data.jobId}`, "success");
                if (data.jobPath) {
                    window.open(new URL(data.jobPath, location.origin).toString(), "_blank");
                }
            } catch (e) {
                console.warn(e);
                this.builder.error = e.message;
                this.pushToast(`Run failed: ${e.message}`);
            } finally {
                this.builder.running = false;
                done();
            }
        },
    };
}
