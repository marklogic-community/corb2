var app = angular.module('dashboard',[]);
app.controller('mainCtrl', ['$scope', '$http','$interval',
                            function($scope, $http, $interval) {
		var hostData=[["localhost","8010"],["localhost","8011"],["localhost","8012"],["localhost","8013"],["localhost","8014"],["localhost","8015"]]
		$scope.availableServers=[]
		$scope.availableServerData=[]
		$scope.pauseButtonText={}
		$scope.pauseButtonStyle={}
		$scope.threadCounts={}
		$scope.allThreadCounts=[]
		for( var i =1;i<=64;i++){
			$scope.allThreadCounts.push(i)
		}
		$scope.pauseResumeButtonClick = function(job){
			
    		var reqStr = ""
    		if(job.paused == "true"){
    			reqStr= "&paused=false"
    		}
    		else{
    			reqStr= "&paused=true"
    		}
    		$http.get("http://"+job.host+":"+job.port+"/service?concise=true"+reqStr)
    	};
    	$scope.updateThreadCount = function(job){
    		var reqStr = "&threads="+$scope.threadCounts[job.host+job.port]
    		$http.get("http://"+job.host+":"+job.port+"/service?concise=true"+reqStr)
    	};
    	var invokeService=function(host,port){
    		var promise=$interval(function() {
        		$http.get("http://"+host+":"+port+"/service?concise=true").success(loadData).error(handleError);
        	}, 5000); 
    		var handleError=function (error, status){
                if( status == "404"){
                	$interval.cancel(promise);
                	console.log("host "+host+" port "+port+" not open")
                }
        	}	
    		var loadData = function(response) {
        		$scope.isLoading=false;
        		console.log("host "+host+" port "+port+" FOUND")
        		var job = response.job;   
        		var oldData=$scope.availableServers[host+":"+port]
        		$scope.availableServers[host+":"+port]=(job)    
        		if(job.paused == "true"){
        			$scope.pauseButtonText[host+port]= "Resume Corb Job"
        			$scope.pauseButtonStyle[host+port]="btn-info"
        		}
        		else{
        			$scope.pauseButtonText[host+port]= "Pause Corb Job"
        			$scope.pauseButtonStyle[host+port]="btn-success"
        		}
        		$scope.threadCounts[host+port]?null:$scope.threadCounts[host+port]=job.currentThreadCount
        		for (i in $scope.availableServerData)
        			if(oldData == $scope.availableServerData[i])
        				$scope.availableServerData.splice(i,1)
        		
        		
        		$scope.availableServerData.push(job)
        	};
    	}
		for (i in hostData)
		{
			invokeService(hostData[i][0],hostData[i][1])
			console.log("hostData[i][0],hostData[i][1] "+hostData[i][0]+hostData[i][1])
		}
}]);
