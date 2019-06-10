xquery version "1.0-ml";
declare namespace bem="http://bem.corb.developer.marklogic.com";
declare namespace p = "http://persistence.corb.developer.marklogic.com"

declare variable $METADATA as xs:string external;
declare variable $URIS_BATCH_REF as xs:string external;
declare variable $BATCH_ID as xs:string external;
declare variable $URIS_TOTAL_COUNT as xs:string external;

let $corb-loader := xdmp:unquote($METADATA)/corb-loader
let $loader-content := $corb-loader/content
let $doc := 
    if ($loader-content/@base64Encoded[. eq "true"]) then 
        xdmp:unquote(xdmp:base64-decode($loader-content))
    else 
        $loader-content/node()

let $metadata := $corb-loader/metadata
let $originalFilename := fn:tokenize($metadata/filename, "\\|/")[last()]  

let $parent-uri :=
    cts:uris((),("limit=1"),
        cts:and-query((
            cts:collection-query($BATCH_ID),
            cts:element-value-query(xs:QName("p:transactionFileName"),$originalFilename)
        ))
    )
let $parentId := fn:tokenize(fn:tokenize($parent-uri,"/")[fn:last()],"\.")[1]

let $child-count :=
    xdmp:estimate(cts:search(/p:stagingTransactionMessage,
        cts:and-query((
            cts:collection-query($BATCH_ID),
            cts:element-value-query(xs:QName("p:fileName"),$originalFilename),
            cts:element-value-query(xs:QName("p:parentTransactionFileHandlingObjectIdentifier"),$parentId)
        ))
    )
return 
  (
    xdmp:log("Finished processing " || xdmp:estimate(collection($parentId)) || " files from: " || $originalFilename || " with parentId: " || $parentId),
    xdmp:collection-delete($BATCH_ID),
    "POST-BATCH-MODULE,"||$URIS_TOTAL_COUNT||","||$child-count||($URIS_TOTAL_COUNT eq xs:String($child-count))
  ) 