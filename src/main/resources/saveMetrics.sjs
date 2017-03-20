declareUpdate();
var insertDoc = function(metricsDocumentStr,collection,uriRoot){
  return function(){
  var json = JSON.parse(metricsDocumentStr);
var jobName= json["job"]["name"];
if(!jobName) {jobName = json["job"]["runLocation"]};
  var coll=[]
  if(collection != "NA"){
    coll.push( fn.tokenize(collection , ",").toArray());
  }
  coll.push(jobName)
 var dateTime = fn.currentDateTime();
        var uri_root=uriRoot;
        if(uriRoot == "NA") uri_root = "/ServiceMetrics/"
        if(!fn.startsWith(uri_root,"/")) uri_root="/"+$uri_root
        if(fn.endsWith(uri_root,"/"))  uri_root = uri_root+"/";
        var uri = uri_root+"CoRB2/"+
                    jobName+"/"+
                    fn.yearFromDateTime(dateTime)+"/"+
                    fn.monthFromDateTime(dateTime)+"/"+
                    fn.dayFromDateTime(dateTime)+"/"+
                    fn.hoursFromDateTime(dateTime)+"/"+
                    fn.minutesFromDateTime(dateTime)+"/"+
                    xdmp.random()+".json";
        xdmp.documentInsert(uri, json, xdmp.defaultPermissions(),coll)
  }
};
var database = xdmp.database(dbName)
xdmp.invokeFunction(insertDoc(metricsDocumentStr,collections,uriRoot),
    {transactionMode:"update-auto-commit", database:database});
