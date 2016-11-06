xquery version "1.0-ml";

declare variable $URIS_BATCH_REF as xs:string external;

(: obtain the filename from source XML file :)
let $originalFilename := fn:tokenize($URIS_BATCH_REF, "\\|/")[last()]

(: generate a unique ID for this batch :)
let $id := fn:string(xdmp:random(10000000000))

return
    (
        xdmp:log("running preBatch.xqy for " || $URIS_BATCH_REF),
        xdmp:set-server-field("com.marklogic.developer.corb.StreamingXMLUrisLoader." || $URIS_BATCH_REF || ".parentId", $id),
        xdmp:set-server-field("com.marklogic.developer.corb.StreamingXMLUrisLoader." || $URIS_BATCH_REF || ".originalFilename", $originalFilename)
    )
