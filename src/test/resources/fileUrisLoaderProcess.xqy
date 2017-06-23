xquery version "1.0-ml";

declare variable $URI as xs:string external;

(:~
 : Decodes a base64 encoded value, and determines
 : what type of node to produce: binary, XML, JSON, text.
 : NOTE: HTML will be repaired to be well-formed XML.
 :)
declare function local:decode($content)
    as document-node()
{
    let $binary := binary { xs:hexBinary(xs:base64Binary($content)) }
    let $content-type := xdmp:document-filter($binary)/*/*/*:meta[@name="content-type"]/@content
    return
      if (starts-with($content-type,"text/")) then
        let $decoded := xdmp:base64-decode($content)
        return
          try {
            (: XML or JSON :)
            xdmp:unquote($decoded)
          } catch ($e2) {
            if ($content-type eq "text/html") then
              (: produce well-formed HTML elements :)
              xdmp:unquote($decoded, (), ("format-xml", "repair-full"))
            else
              xdmp:unquote($decoded, (), "format-text")
          }
      else
        document { $binary }
};

(: first, unqote the corb-loader XML string and select the document element :)
let $loader := xdmp:unquote($URI)/*
(: either decode the base64 encoded string, or select the XML content :)
let $doc :=
    if ($loader/content/@base64Encoded[. eq "true"]) then
        local:decode($loader/content)
    else
        $loader/content/data()

let $uri := "/test/loader/" || $loader/metadata/filename

return (
    string-join(($uri, xdmp:node-kind($doc/node())), ","),
    xdmp:document-insert($uri, $doc, (), "file-uris-loader")
  )
