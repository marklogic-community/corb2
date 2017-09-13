var app = angular.module("dashboard",[]);
app.controller("mainCtrl", ["$scope", "$http","$interval",
    function($scope, $http, $interval) {

        var metricsPath = "/metrics";
        var host = location.hostname || "localhost";
        var port = location.port;
        var promises = {};

        $scope.availableServers = [];
        $scope.availableServerData = [];
        $scope.pauseButtonText = {};
        $scope.pauseButtonStyle = {};
        $scope.threadCounts = {};
        $scope.allThreadCounts = [];

        for (var i =1; i <= 64; i++) {
            $scope.allThreadCounts.push(i);
        }

        $scope.pauseResumeButtonClick = function(job){
            var reqStr = "command=";
            if (job.paused === true) {
                reqStr += "resume";
            } else {
                reqStr += "pause";
            }
            $http.post("http://" + job.host + ":" + job.port + "/" + job.id + metricsPath + "?" + reqStr, {"headers":{"Content-Type": "application/x-www-form-urlencoded"}}).then(loadData, handleError);
        };

        $scope.updateThreadCount = function(job){
            var reqStr = "thread-count=" + $scope.threadCounts[job.id];
            $http.post("http://" + job.host + ":" + job.port + "/" + job.id + metricsPath + "?" + reqStr, {"headers":{"Content-Type": "application/x-www-form-urlencoded"}}).then(loadData, handleError);
        };

        $scope.openJob = function(job){
            window.open("http://" + job.host + ":" + job.port + "/" + job.id , "target=_blank");
        };

        var pad = function (n, z) {
            z = z || 2;
            return ("00" + n).slice(-z);
        };

        $scope.msToTime = function(s) {
            var ms = s % 1000;
            s = (s - ms) / 1000;
            var secs = s % 60;
            s = (s - secs) / 60;
            var mins = s % 60;
            var hrs = (s - mins) / 60;
            return pad(hrs) + ":" + pad(mins) + ":" + pad(secs);
        };

        var isNumeric = function isNumeric(value) {
            return !isNaN(value - parseFloat(value));
        };

        var handleError = function (response){

            if (response.status === "404" || response.status === -1) {
                $interval.cancel(promises[response.config.url]);
            }
        };

        var loadData = function(response) {
            $scope.isLoading = false;
            //ensure that this works with an array or single job object
            var jobs = [].concat(response.data);
            for (var jobIndex in jobs) {
                var job = jobs[jobIndex].job;
                var oldData = $scope.availableServers[job.id];
                $scope.availableServers[job.id] = (job);
                if (job.paused === true) {
                    $scope.pauseButtonText[job.id] = "Resume Corb Job";
                    $scope.pauseButtonStyle[job.id] = "btn-info";
                } else {
                    $scope.pauseButtonText[job.id] = "Pause Corb Job";
                    $scope.pauseButtonStyle[job.id] = "btn-success";
                }
                $scope.threadCounts[job.id] ? null : $scope.threadCounts[job.id] = job.currentThreadCount;
                for (var i in $scope.availableServerData) {
                    if (oldData === $scope.availableServerData[i]) {
                        $scope.availableServerData.splice(i, 1);
                    }
                }
                $scope.availableServerData.push(job);
            }
        };

        var invokeService = function(host, port) {
            var metricsUrl = "http://" + host + ":" + port + metricsPath;
            $http.get(metricsUrl).then(loadData, handleError);
            promises[metricsUrl] = $interval(function() {
                $http.get(metricsUrl).then(loadData, handleError);
            }, 5000);
        };

        $scope.parsePorts = function() {
            var hostData= [];
            var items = $scope.ports.split(",");
            for (var i = 0, len = items.length; i < len; i++) {
                var portToken = items[i];
                // when there is a dash, process the range of values (inclusive)
                if (portToken.includes("-")) {
                    var range = portToken.split("-");
                    for (var port = range[0]; port <= range[1]; port++) {
                        hostData.push([host, port]);
                    }
                    // otherwise just the specific port number
                } else {
                    if (isNumeric(portToken)) {
                        hostData.push([host, portToken]);
                    }
                }
            }
            // See if there are any jobs running on other ports
            for (var index in hostData) {
                invokeService(hostData[index][0], hostData[index][1]);
            }
        };

        //See if there are any jobs running on this webserver
        invokeService(host, port);
    }]);
