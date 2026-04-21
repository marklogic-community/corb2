xquery version "1.0-ml";

declare variable $URI as xs:string external;

let $payload := fn:string-join(for $i in 1 to 80 return "x", "")
return fn:concat("line-", $URI, ",", $payload)
