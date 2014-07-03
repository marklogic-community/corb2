xquery version "1.0-ml";

declare variable $URI as xs:string external;
 
fn:doc($URI)/node()