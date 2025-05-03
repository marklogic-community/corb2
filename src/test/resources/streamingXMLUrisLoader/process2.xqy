xquery version "1.0-ml";
declare namespace bem = "http://bem.corb.developer.marklogic.com";
declare namespace p = "http://persistence.corb.developer.marklogic.com";

declare variable $URI as xs:string external;
declare variable $URIS_BATCH_REF as xs:string external;
declare variable $BATCH_ID as xs:string external;

let $corb-loader := xdmp:unquote($URI)/corb-loader
let $loader-content := $corb-loader/content
let $doc :=
    if ($loader-content/@base64Encoded[. eq "true"]) then
        xdmp:unquote(xdmp:base64-decode($loader-content))
    else
        $loader-content/bem:*

let $metadata := $corb-loader/metadata
let $originalFilename := fn:tokenize($metadata/filename, "\\|/")[last()]

let $random := xdmp:random(1000)
let $prandom := if($random lt 10) then "00"||$random else if ($random lt 100) then "0"||$random else xs:string($random)
let $id := (current-dateTime() - xs:dateTime("1970-01-01T00:00:00-00:00")) div xs:dayTimeDuration('PT0.000001S') || $prandom

let $parent-uri :=
    cts:uris((),("limit=1"),
        cts:and-query((
            cts:collection-query($BATCH_ID),
            cts:element-value-query(xs:QName("p:transactionFileName"),$originalFilename)
        ))
    )
let $parentId := fn:tokenize(fn:tokenize($parent-uri,"/")[fn:last()],"\.")[1]

let $content :=
    <stagingTransactionMessage xmlns="http://persistence.corb.developer.marklogic.com" xmlns:base="http://base.corb.developer.marklogic.com">
      <base:objectIdentifier>{ $id }</base:objectIdentifier>
      <base:lastModified>{ fn:current-dateTime() }</base:lastModified>
      <base:objectCreationDateTime>{ fn:current-dateTime() }</base:objectCreationDateTime>
      <fileName>{ $originalFilename }</fileName>
      <messageXML>{ $doc }</messageXML>
      <status>L</status>
      <parentTransactionFileHandlingObjectIdentifier>{ $parentId }</parentTransactionFileHandlingObjectIdentifier>
    </stagingTransactionMessage>

let $collections := ($BATCH_ID)
let $uri := "/streamingXMLUrisLoader/StagingTransactionMessage/" || $id || ".xml"

return (
  xdmp:log("running process.xqy for " || $URIS_BATCH_REF || " " || $uri),
  xdmp:document-insert($uri, $content, (), $collections),
  if ($doc) then $uri else ()
)
