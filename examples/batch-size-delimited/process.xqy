xquery version "1.0-ml";

declare variable $URI as xs:string external;

let $uris := fn:tokenize($URI, "\\|")
return fn:concat(
    "batch(",
    fn:count($uris),
    "): ",
    fn:string-join($uris, " | ")
)
