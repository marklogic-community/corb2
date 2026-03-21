xquery version "1.0-ml";

declare variable $URI as xs:string external;

fn:concat("line-", $URI, ",dummy text for split-by-lines")
