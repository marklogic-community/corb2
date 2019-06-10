xquery version "1.0-ml";
declare namespace bem="http://bem.corb.developer.marklogic.com";

declare variable $METADATA as xs:string external;
declare variable $URIS_BATCH_REF as xs:string external;
declare variable $BATCH_ID as xs:string external;

let $corb-loader := xdmp:unquote($METADATA)/corb-loader
let $loader-content := $corb-loader/content
let $doc := 
    if ($loader-content/@base64Encoded[. eq "true"]) then 
        xdmp:unquote(xdmp:base64-decode($loader-content))
    else 
        $loader-content/node()

let $metadata := $corb-loader/metadata
let $originalFilename := fn:tokenize($metadata/filename, "\\|/")[last()]  
        
let $random := xdmp:random(1000)
let $prandom := if($random lt 10) then "00"||$random else if ($random lt 100) then "0"||$random else xs:string($random)
let $id := (current-dateTime() - xs:dateTime("1970-01-01T00:00:00-00:00")) div xs:dayTimeDuration('PT0.000001S') || $prandom

let $content := 
    <transactionFileHandling xmlns="http://persistence.corb.developer.marklogic.com" xmlns:base="http://base.corb.developer.marklogic.com" xmlns:p="http://persistence.base.corb.developer.marklogic.com">
        <base:objectIdentifier>{ $id }</base:objectIdentifier>
        <base:lastModified>{ fn:current-dateTime() }</base:lastModified>
        <base:versionInformation>
          <p:isCurrentVersion>true</p:isCurrentVersion>
          <p:versionNumber>1</p:versionNumber>
          <p:versionDate>{ fn:current-dateTime() }</p:versionDate>
        </base:versionInformation>
        <transactionFileName>{ $originalFilename }</transactionFileName>
        <loadTime>{ fn:current-dateTime() }</loadTime>
        <fileInformationXML>{ $doc }</fileInformationXML>
        <definingTransactionFileStatusType>
          <p:referenceTypeName>TransactionMessageStatusType</p:referenceTypeName>
          <p:referenceTypeCode>L</p:referenceTypeCode>
          <p:referenceTypeCodeName>Loaded</p:referenceTypeCodeName>
        </definingTransactionFileStatusType>
        <loadRecordQuantity>5</loadRecordQuantity>
   </transactionFileHandling>

let $collections := ($BATCH_ID)      
let $uri := "/streamingXMLUrisLoader/TransactionFileHandling/" || $id || ".xml"

return (
    xdmp:log("running pre-batch.xqy for " || $URIS_BATCH_REF || " " || $uri),
    xdmp:document-insert($uri, $content, (), $collections), 
    $uri 
)

