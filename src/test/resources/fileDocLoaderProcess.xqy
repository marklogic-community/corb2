xquery version "1.0-ml";

declare variable $DOC as document-node() external;

(:~
 : Decodes a base64 encoded value, and determines
 : what type of node to produce: binary, XML, JSON, text.
 : NOTE: HTML will be repaired to be well-formed XML.
 :)
declare function local:decode($content)
    as document-node()
{
     try {
        let $decoded := xdmp:base64-decode($content)
        return
          try {
            (: XML or JSON :)
            xdmp:unquote($decoded)
          } catch ($e2) {
            try {
              (: produce well-formed HTML elements :)
              xdmp:unquote($decoded, (), ("format-xml", "repair-full"))
            } catch($e2) {
              xdmp:unquote($decoded, (), "format-text")
            }
         }
     } catch ($e1) {
       document { binary { xs:hexBinary(xs:base64Binary($content)) } }
     }
};

let $content := $DOC/*/content
(: either decode the base64 encoded string, or select the XML content :)
let $doc :=
    if ($content/@base64Encoded[. eq "true"]) then
        local:decode($content)
    else
        $content/data()

let $uri := "/test/loader/" || $DOC/*/metadata/filename

return (
    string-join(($uri, xdmp:node-kind($doc/node())), ","),
    xdmp:document-insert($uri, $doc, (), "file-uris-loader")
  )
