var app = angular.module('dashboard',[]);
app.controller('mainCtrl', ['$scope', '$http','$interval',
                            function($scope, $http, $interval) {
    	var loadData = function(response) {
    		$scope.isLoading=false;
    		var job = response.job;   
    		if(job.paused == "true"){
    			$scope.pauseButtonText= "Resume Corb Job"
    			$scope.pauseButtonStyle="btn-info"
    		}
    		else{
    			$scope.pauseButtonText= "Pause Corb Job"
    			$scope.pauseButtonStyle="btn-success"
    		}
    		$scope.successPercent = (job.numberOfSucceededTasks && job.numberOfSucceededTasks >0?((job.numberOfSucceededTasks/job.totalNumberOfTasks) * 100): 0)
    		$scope.successPercent =Math.round($scope.successPercent * 100) / 100
    		$scope.successTotals = (job.numberOfSucceededTasks?job.numberOfSucceededTasks:0) +" out of "+job.totalNumberOfTasks +" succeeded."
    		$scope.failedPercent = (job.numberOfFailedTasks && job.numberOfFailedTasks > 0 ?((job.numberOfFailedTasks/job.totalNumberOfTasks) * 100) :0)
    		$scope.failedPercent = Math.round($scope.failedPercent * 100) / 100
    		$scope.failedTotals = (job.numberOfFailedTasks?job.numberOfFailedTasks:0) +" out of "+ job.totalNumberOfTasks +" failed."
    		$scope.jobDuration = (job.totalRunTimeInMillis && job.totalRunTimeInMillis>0 ) ? msToTime(job.totalRunTimeInMillis) : "Not Running";
    		$scope.averageTransactionTimeInMillis =  Math.round(job.averageTransactionTimeInMillis * 100) / 100
    		$scope.job = job
    		if(job.userProvidedOptions){
    			$scope.userProvidedOptions = job.userProvidedOptions;//save this as this is fetched only once
    		}
    		
    	};
    	var handleError=function (error, status){
            if( status == "404"){
            	$interval.cancel(promise);
            	$scope.allDone=100;
            	$scope.successPercent = 0;
            	$scope.failedPercent = 0;	
            	$scope.pauseButtonText= "Corb Job Completed"
        		$scope.pauseButtonStyle="disabled"
            }
    	}
    	var promise=$interval(function() {
    		$http.get("/service?json=true&concise=true").success(loadData).error(handleError);
    	}, 1000); 
    	var msToTime=function(s) {
    		  var ms = s % 1000;
    		  s = (s - ms) / 1000;
    		  var secs = s % 60;
    		  s = (s - secs) / 60;
    		  var mins = s % 60;
    		  var hrs = (s - mins) / 60;

    		  return hrs + ':' + mins + ':' + secs + '.' + ms;
    		};
    	$scope.pauseResumeButtonClick = function(){
    		var reqStr = ""
    		if($scope.job.paused == "true"){
    			reqStr= "&paused=false"
    		}
    		else{
    			reqStr= "&paused=true"
    		}
    		$http.get("/service?json=true"+reqStr).success(loadData)
    	};
		$http.get("/service?json=true").success(loadData).error(handleError);

}]);
