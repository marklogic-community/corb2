"use strict";
var app = angular.module("dashboard", []);
app.controller("mainCtrl", ["$scope", "$http", "$interval",
    function($scope, $http, $interval) {

        var metricsPath = "/metrics";
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
            $http.post(toUrl(job) + metricsPath + "?" + commandActionParameter(job)).then(loadData, handleError);
        };

        $scope.updateThreadCount = function(job) {
            var reqStr = "thread-count=" + $scope.threadCounts[job.id];
            $http.post(toUrl(job) + metricsPath + "?" + reqStr).then(loadData, handleError);
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
