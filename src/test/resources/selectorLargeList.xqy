xquery version "1.0-ml";
declare variable $count as xs:string external;

let $max := xs:int($count)
return ("uris_batch_ref_value", $max, for $i in 1 to $max return "http://www.test.com/some/random/path/" || $i || ".xml")