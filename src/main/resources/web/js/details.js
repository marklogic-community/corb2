"use strict";
var app = angular.module("details", []);
app.controller("mainCtrl", ["$scope", "$http", "$interval",
    function($scope, $http, $interval) {

        var serviceUrl = location.href + "/metrics";
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

        var scheduleUpdates = function() {
            //Start polling for job stats updates
            promise = $interval(function() {
                var concise = isNaN(+$scope.totalNumberOfTasks) && typeof $scope.job.totalNumberOfTasks === "undefined" ? "" : "?concise";
                $http.get(serviceUrl + concise).then(loadData, handleError);
            }, 5000);
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

        var handleCommandResponse = function(response) {
            loadData(response);
            scheduleUpdates();
        };

        $scope.pauseResumeButtonClick = function(){
            $interval.cancel(promise);
            $scope.loading = true;
            $http.post(serviceUrl + "?concise=true&" + commandActionParameter($scope.job)).then(handleCommandResponse, handleError);
        };

        $scope.updateThreadCount = function(){
            $interval.cancel(promise);
            $scope.loading = true;
            $scope.updateThreadButtonStyle = "btn glyphicon glyphicon-refresh";
            $http.post(serviceUrl + "?concise=true&thread-count=" + $scope.threadCount).then(handleCommandResponse, handleError);
        };

        $scope.updateThreadsButtonStyle = "btn-primary";
        $scope.allThreadCounts = [];
        for (var i = 1; i <= 64; i++) {
            $scope.allThreadCounts.push(i);
        }

        $http.get(serviceUrl).then(loadData, handleError);
        scheduleUpdates();
}]);
