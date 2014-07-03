xquery version "1.0-ml";

 declare variable $URIS_BATCH_REF as xs:string external;
 
 let $_ := xdmp:log("post-batch: URIS_BATCH_REF: " || $URIS_BATCH_REF)
 return 'success'