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

var app = angular.module("corbApp", []);
app.controller("dashboardCtrl", ["$scope", "$http", "$interval",
    function($scope, $http, $interval) {

        var metricsPath = "?format=json";
        var host = location.hostname || "localhost";
        var port = location.port;
        var promises = {};

        var loadData = function(response) {
            //ensure that this works with an array or single job object
            var jobs = [].concat(response.data.jobs || response.data );
            for (var jobIndex in jobs) {
                var job = jobs[jobIndex].job;
                var oldData = $scope.availableServers[job.id];
                $scope.availableServers[job.id] = job;
                $scope.threadCounts[job.id] ? null : $scope.threadCounts[job.id] = job.currentThreadCount;
                for (var i in $scope.availableServerData) {
                    if (oldData === $scope.availableServerData[i]) {
                        $scope.availableServerData.splice(i, 1);
                    }
                }
                $scope.availableServerData.push(job);
            }
        };

        // If a job doesn't exist anymore, or the server cannot be reached - stop checking
        var handleError = function (response){
            if (response.status === "404" || response.status === -1) {
                $interval.cancel(promises[response.config.url]);
            }
        };

        var toUrl = function(job) {
            return "http://" + job.host + ":" + job.port + "/" + job.id;
        };

        $scope.availableServers = [];
        $scope.availableServerData = [];
        $scope.threadCounts = {};
        $scope.allThreadCounts = [];
        for (var i =1; i <= 64; i++) {
            $scope.allThreadCounts.push(i);
        }
        //seed the list with the current server
        $scope.external = [ {host: host, port: port} ];
        //onClick create a new entry in the array for the user to fill in values
        $scope.addExternal = function() {
            $scope.external.push( {host:null, port:null} );
        };
        //remove this item from the array of URLs to monitor
        $scope.removeExternal = function(i) {
            $scope.external.splice(i, 1);
        };

        $scope.pauseResumeButtonClick = function(job) {
            $http.post(toUrl(job) + metricsPath + "&" + commandActionParameter(job)).then(loadData, handleError);
        };

        $scope.updateThreadCount = function(job) {
            $http.post(toUrl(job) + metricsPath + "&thread-count=" + $scope.threadCounts[job.id]).then(loadData, handleError);
        };

        $scope.openJob = function(job) {
            window.open(toUrl(job), "target=_blank");
        };

        var scheduleMetricsRefresh = function(host, port) {
            var metricsUrl = "http://" + host + ":" + port + metricsPath;
            $http.get(metricsUrl).then(loadData, handleError);
            promises[metricsUrl] = $interval(function() {
                $http.get(metricsUrl).then(loadData, handleError);
            }, 5000);
        };

        $scope.parseExternalHostAndPorts = function($i) {
            var hostData= [];
            var externalHost = $scope.external[$i].host;
            var externalPorts = $scope.external[$i].port;

            if (externalHost && externalPorts) {

                var matches = externalHost.match("^(https?\\:)?\\/?\\/?(([^:\\/?#]*)(?:\\:([0-9]+))?)([\\/]{0,1}[^?#]*)(\\?[^#]*|)(#.*|)$");
                externalHost = matches[3];
                var items = externalPorts.split(",");
                for (var i = 0, len = items.length; i < len; i++) {
                    var portToken = items[i];
                    // when there is a dash, process the range of values (inclusive)
                    if (portToken.includes("-")) {
                        var range = portToken.split("-");
                        for (var port = range[0]; port <= range[1]; port++) {
                            hostData.push([externalHost, port]);
                        }
                        // otherwise just the specific port number
                    } else if (isNumeric(portToken)) {
                        hostData.push([externalHost, portToken]);
                    }
                }
                // See if there are any jobs running on other ports
                for (var index in hostData) {
                    scheduleMetricsRefresh(hostData[index][0], hostData[index][1]);
                }
            }
        };

        //See if there are any jobs running on this webserver
        scheduleMetricsRefresh(host, port);
    }]);

app.controller("jobCtrl", ["$scope", "$http", "$interval",
    function($scope, $http, $interval) {

        var serviceUrl = location.protocol + '//' + location.host + location.pathname + "?format=json";
        var promise;

        var handleError = function (error, status) {
            if (status === "404" || status === -1) {
                $interval.cancel(promise);
                $scope.allDone = 100;
                $scope.successPercent = 0;
                $scope.failedPercent = 0;
            }
            $scope.pauseButtonText = "Completed";
            $scope.pauseButtonStyle = "disabled";
            $scope.updateThreadsButtonStyle = "disabled";
        };

        var loadData = function(response) {
            var job = response.data.job;
            $scope.job = job;
            $scope.loading = false;

            if (job.userProvidedOptions) {
                $scope.userProvidedOptions = job.userProvidedOptions;//save this as this is fetched only once
            }

            if (typeof job.totalNumberOfTasks !== "undefined" && job.totalNumberOfTasks > 0) {
                $scope.initTaskTimeInMillis = job.initTaskTimeInMillis;
                $scope.urisLoadTimeInMillis = job.urisLoadTimeInMillis;
                $scope.preBatchRunTimeInMillis = job.preBatchRunTimeInMillis;
                $scope.totalNumberOfTasks = job.totalNumberOfTasks;
            }
            $scope.threadCount ? null : $scope.threadCount = job.currentThreadCount;
            $scope.successPercent = (job.numberOfSucceededTasks && job.numberOfSucceededTasks > 0 ? ((job.numberOfSucceededTasks / $scope.totalNumberOfTasks) * 100) : 0);
            $scope.successPercent = Math.round($scope.successPercent * 100) / 100;
            $scope.successTotals = (job.numberOfSucceededTasks ? job.numberOfSucceededTasks : 0) + " out of " + $scope.totalNumberOfTasks + " succeeded.";
            $scope.failedPercent = (job.numberOfFailedTasks && job.numberOfFailedTasks > 0 ? ((job.numberOfFailedTasks/$scope.totalNumberOfTasks) * 100) : 0);
            $scope.failedPercent = Math.round($scope.failedPercent * 100) / 100;
            $scope.failedTotals = (job.numberOfFailedTasks ? job.numberOfFailedTasks : 0) + " out of " + $scope.totalNumberOfTasks + " failed.";
            $scope.jobDuration = (job.totalRunTimeInMillis && job.totalRunTimeInMillis > 0 ) ? msToTime(job.totalRunTimeInMillis) : "Not Running";
            $scope.averageTransactionTimeInMillis =  Math.round(job.averageTransactionTimeInMillis * 100) / 100;
            if (job.numberOfSucceededTasks+ job.numberOfFailedTasks >= $scope.totalNumberOfTasks) {
                $scope.jobStatus = "completed";
                $scope.pauseButtonText = $scope.jobStatus;
                $scope.pauseButtonStyle = "disabled";
                $scope.updateThreadsButtonStyle = "disabled";
            } else if ($scope.job.paused) {
                $scope.pauseButtonText = "resume";
                $scope.pauseButtonStyle = "btn-info";
            } else {
                $scope.pauseButtonText = "pause";
                $scope.pauseButtonStyle = "btn-success";
            }
        };

        var scheduleUpdates = function() {
            //Start polling for job stats updates
            promise = $interval(function() {
                var concise = isNaN(+$scope.totalNumberOfTasks) && typeof $scope.job.totalNumberOfTasks === "undefined" ? "" : "&concise";
                $http.get(serviceUrl + concise).then(loadData, handleError);
            }, 5000);
        };

        var handleCommandResponse = function(response) {
            loadData(response);
            scheduleUpdates();
        };

        $scope.pauseResumeButtonClick = function(){
            $interval.cancel(promise);
            $scope.loading = true;
            $http.post(serviceUrl + "&concise=true&" + commandActionParameter($scope.job)).then(handleCommandResponse, handleError);
        };

        $scope.updateThreadCount = function(){
            $interval.cancel(promise);
            $scope.loading = true;
            $scope.updateThreadButtonStyle = "btn glyphicon glyphicon-refresh";
            $http.post(serviceUrl + "&concise=true&thread-count=" + $scope.threadCount).then(handleCommandResponse, handleError);
        };

        $scope.updateThreadsButtonStyle = "btn-primary";
        $scope.allThreadCounts = [];
        for (var i = 1; i <= 64; i++) {
            $scope.allThreadCounts.push(i);
        }

        $http.get(serviceUrl).then(loadData, handleError);
        scheduleUpdates();
    }]);
