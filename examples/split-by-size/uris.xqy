xquery version "1.0-ml";

let $uris := for $i in 1 to 12 return xs:string($i)
return (fn:count($uris), $uris)
