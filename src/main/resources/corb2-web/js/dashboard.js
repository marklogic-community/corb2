var app = angular.module('dashboard',[]);
app.controller('mainCtrl', ['$scope', '$http','$interval',
                            function($scope, $http, $interval) {
    	var loadData = function(response) {
    		$scope.isLoading=false;
    		$scope.job = response.job;   
    		if($scope.job.paused == "true"){
    			$scope.pauseButtonText= "Resume Corb Job"
    		}
    		else{
    			$scope.pauseButtonText= "Pause Corb Job"
    		}
    	}
		$http.get("/service?json=true").success(loadData)
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
}]);
