xquery version "1.0-ml";

declare variable $URI as xs:string external;

let $loader := xdmp:unquote($URI)
let $path := fn:string($loader/corb-loader/metadata/path)
let $contentType := fn:string($loader/corb-loader/metadata/content-type)
let $base64Length := fn:string(fn:string-length(fn:string($loader/corb-loader/content)))
return fn:concat($path, ",", $contentType, ",", $base64Length)
