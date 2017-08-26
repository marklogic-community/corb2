var app = angular.module("dashboard",[]);
app.controller("mainCtrl", ["$scope", "$http","$interval",
    function($scope, $http, $interval) {

        var servicePath = "/stats";
        var host = location.hostname || "localhost";

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
            var reqStr = "paused=";
            if (job.paused === true) {
                reqStr = "false";
            } else {
                reqStr = "true";
            }
            $http.post("http://" + job.host + ":" + job.port + servicePath + "?" + reqStr, {"headers":{"Content-Type": "application/x-www-form-urlencoded"}});
        };

        $scope.updateThreadCount = function(job){
            var reqStr = "threads=" + $scope.threadCounts[job.host + job.port];
            $http.post("http://" + job.host + ":" + job.port + servicePath + "?" + reqStr, {"headers":{"Content-Type": "application/x-www-form-urlencoded"}});
        };

        $scope.openJob = function(job){
            window.open("http://" + job.host + ":" + job.port + "/web/", "target=_blank");
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

        var invokeService = function(host, port) {
            var handleError = function (error, status){
                if (status === "404") {
                    $interval.cancel(promise);
                    console.log("host " + host + " port " + port + " not open");
                }
            };
            var loadData = function(response) {
                $scope.isLoading = false;
                console.log("host " + host + " port " + port + " FOUND");

                var job = response.data.job;
                var oldData = $scope.availableServers[host + ":" + port];
                $scope.availableServers[host + ":" + port] = (job);
                if (job.paused === true) {
                    $scope.pauseButtonText[host + port] = "Resume Corb Job";
                    $scope.pauseButtonStyle[host + port] = "btn-info";
                } else {
                    $scope.pauseButtonText[host + port] = "Pause Corb Job";
                    $scope.pauseButtonStyle[host + port] = "btn-success";
                }
                $scope.threadCounts[host + port] ? null : $scope.threadCounts[host + port] = job.currentThreadCount;
                for (var i in $scope.availableServerData) {
                    if (oldData === $scope.availableServerData[i]) {
                        $scope.availableServerData.splice(i, 1);
                    }
                }
                $scope.availableServerData.push(job);
            };
            var promise = $interval(function() {
                $http.get("http://" + host + ":" + port + servicePath).then(loadData, handleError);
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
                    for (var port = range[0]; port <= range[1]; j++) {
                        hostData.push([host, port]);
                    }
                    // otherwise just the specific port number
                } else {
                    hostData.push([host, portToken]);
                }
            }
            for (var job in hostData) {
                invokeService(job[i][0], job[i][1]);
            }
        };
    }]);
