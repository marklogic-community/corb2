xquery version "1.0-ml";

declare variable $URI as xs:string external;

fn:concat("processed URI: ", $URI)
