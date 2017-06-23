xquery version "1.0-ml";
declare variable $URIS_BATCH_REF as xs:string external;

let $parentId := xdmp:get-server-field("com.marklogic.developer.corb.StreamingXMLUrisLoader." || $URIS_BATCH_REF || ".parentId")
let $originalFilename  := xdmp:get-server-field("com.marklogic.developer.corb.StreamingXMLUrisLoader." || $URIS_BATCH_REF || ".originalFilename")

(: cleaning up after the test is finished :)   
return 
  (
    xdmp:log("Finished processing " || xdmp:estimate(collection($parentId)) || " files from: " || $originalFilename || " with parentId: " || $parentId),
    xdmp:collection-delete($parentId) 
  )   