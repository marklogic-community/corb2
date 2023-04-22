declareUpdate();

var dbName;
var metricsDocumentStr;
var uriRoot;
var collections;

const insertDoc = function(metricsDocumentStr, collection, uriRoot){
  return function(){
    const json = JSON.parse(metricsDocumentStr);
    const jobName = json["job"]["name"];
    if (!jobName) { jobName = json["job"]["runLocation"]; }
    if (fn.startsWith(jobName, "/")) {
      jobName=fn.substring(jobName, 2);
    }
    const coll = [];
    if (collection != "NA") {
      coll.push( fn.tokenize(collection , ",").toArray());
    }
    coll.push(jobName);
    const dateTime = fn.currentDateTime();
    let uri_root = uriRoot;
    if (uriRoot == "NA") { uri_root = "/ServiceMetrics/"; }
    if (!fn.startsWith(uri_root, "/")) { uri_root = "/" + $uri_root; }
    if (fn.endsWith(uri_root, "/"))  { uri_root = uri_root + "/"; }
    let orig_uri = json["job"]["metricsDocUri"];
    let uri = orig_uri;
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
const database = xdmp.database(dbName);
xdmp.invokeFunction(insertDoc(metricsDocumentStr, collections, uriRoot),
    {transactionMode:"update-auto-commit", database:database});
