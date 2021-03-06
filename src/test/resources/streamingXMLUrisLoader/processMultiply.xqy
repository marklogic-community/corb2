xquery version "1.0-ml";
declare namespace bem="http://bem.corb.developer.marklogic.com";

declare variable $URI as xs:string external;
declare variable $URIS_BATCH_REF as xs:string external;
declare variable $COPIES as xs:string external;

let $doc := xdmp:unquote($URI)
let $xml := xdmp:quote($doc, 
                <options xmlns="xdmp:quote">
                    <omit-xml-declaration>yes</omit-xml-declaration>
                </options>)
let $corb-loader-content := $doc/corb-loader/content
let $content :=
    if ($corb-loader-content) then
        xdmp:base64-decode($corb-loader-content)
      else
        $xml
return
  for $i in 1 to xs:int($COPIES)
  return $content
      