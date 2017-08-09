declareUpdate();

var dbName;
var metricsDocumentStr;
var uriRoot;
var collections;

var insertDoc = function(metricsDocumentStr, collection, uriRoot){
  return function(){
    var json = JSON.parse(metricsDocumentStr);
    var jobName = json["job"]["name"];
    if (!jobName) { jobName = json["job"]["runLocation"]; }
    if (fn.startsWith(jobName,"/")) {
      jobName=fn.substring(jobName,2);
    }
    var coll = [];
    if (collection != "NA") {
      coll.push( fn.tokenize(collection , ",").toArray());
    }
    coll.push(jobName);
    var dateTime = fn.currentDateTime();
    var uri_root = uriRoot;
    if (uriRoot == "NA") { uri_root = "/ServiceMetrics/"; }
    if (!fn.startsWith(uri_root,"/")) { uri_root="/"+$uri_root; }
    if (fn.endsWith(uri_root,"/"))  { uri_root = uri_root+"/"; }
    var orig_uri = json["job"]["metricsDocUri"];
    var uri = orig_uri;
    if (!uri) { 
      uri = uri_root + "CORB/" +
                    jobName + "/" +
                    fn.yearFromDateTime(dateTime) + "/" +
                    fn.monthFromDateTime(dateTime) + "/" +
                    fn.dayFromDateTime(dateTime) + "/" +
                    fn.hoursFromDateTime(dateTime) + "/" +
                    fn.minutesFromDateTime(dateTime) + "/" +
                    xdmp.random() + ".json";
      orig_uri = uri;        
    }
    else if (!json["job"]["endTime"]) {//Job finished so update the root document
        	uri = uri + "/" + xdmp.random();
    }
    xdmp.documentInsert(uri, json, xdmp.defaultPermissions(), coll);
    return orig_uri;
  }
};
var database = xdmp.database(dbName);
xdmp.invokeFunction(insertDoc(metricsDocumentStr, collections, uriRoot),
    {transactionMode:"update-auto-commit", database:database});
