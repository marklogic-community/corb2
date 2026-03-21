xquery version "1.0-ml";

let $uris := for $i in 1 to 9 return fn:concat("/batch-size-delimited/", $i, ".json")
return (fn:count($uris), $uris)
