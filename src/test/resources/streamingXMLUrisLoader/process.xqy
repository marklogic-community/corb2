xquery version "1.0-ml";
declare namespace bem="http://bem.corb.developer.marklogic.com";

declare variable $URI as xs:string external;
declare variable $URIS_BATCH_REF as xs:string external;

let $doc := xdmp:unquote($URI)
let $parentId := xdmp:get-server-field("com.marklogic.developer.corb.StreamingXMLUrisLoader." || $URIS_BATCH_REF || ".parentId")
let $originalFilename  := xdmp:get-server-field("com.marklogic.developer.corb.StreamingXMLUrisLoader." || $URIS_BATCH_REF || ".originalFilename")
let $id := fn:string(xdmp:random(10000000000))

let $content := 
    if ( $doc[//bem:FileInformation] ) then
        <transactionFileHandling xmlns="http://persistence.ffe.corb.developer.marklogic.com" xmlns:base="http://base.persistence.base.corb.developer.marklogic.com" xmlns:p="http://persistence.base.corb.developer.marklogic.com">
            <base:objectIdentifier>{ $parentId }</base:objectIdentifier>
            <base:lastModified>{ fn:current-dateTime() }</base:lastModified>
            <base:lastModifiedBy>EE_BATCH_USER</base:lastModifiedBy>
            <base:deleted>false</base:deleted>
            <base:versionInformation>
              <p:isCurrentVersion>true</p:isCurrentVersion>
              <p:isDiscardable>false</p:isDiscardable>
              <p:versionNumber>1</p:versionNumber>
              <p:versionDate>{ fn:current-dateTime() }</p:versionDate>
            </base:versionInformation>
            <transactionFileName>{ $originalFilename }</transactionFileName>
            <loadTime>{ fn:current-dateTime() }</loadTime>
            <fileInformationXML>{ $URI }</fileInformationXML>
            <definingTransactionMessageDirection>
              <p:referenceTypeName>TransactionMessageDirectionType</p:referenceTypeName>
              <p:referenceTypeCode>1</p:referenceTypeCode>
              <p:referenceTypeCodeName>Inbound</p:referenceTypeCodeName>
            </definingTransactionMessageDirection>
            <definingTransactionMessageType>
              <p:referenceTypeName>TransactionMessageType</p:referenceTypeName>
              <p:referenceTypeCode>1</p:referenceTypeCode>
              <p:referenceTypeCodeName>834</p:referenceTypeCodeName>
            </definingTransactionMessageType>
            <definingTransactionFileStatusType>
              <p:referenceTypeName>TransactionMessageStatusType</p:referenceTypeName>
              <p:referenceTypeCode>L</p:referenceTypeCode>
              <p:referenceTypeCodeName>Loaded</p:referenceTypeCodeName>
            </definingTransactionFileStatusType>
            <loadRecordQuantity>5</loadRecordQuantity>
          </transactionFileHandling>
    else
        <stagingTransactionMessage xmlns="http://persistence.ffe.corb.developer.marklogic.com" xmlns:base="http://base.persistence.base.corb.developer.marklogic.com">
          <base:objectIdentifier>{ $id }</base:objectIdentifier>
          <base:lastModified>{ fn:current-dateTime() }</base:lastModified>
          <base:lastModifiedBy/>
          <base:deleted>false</base:deleted>
          <base:objectCreationDateTime>{ fn:current-dateTime() }</base:objectCreationDateTime>
          <transactionMessageType>EDI</transactionMessageType>
          <fileName>{ $originalFilename }</fileName>
          <messageXML>{ $URI }</messageXML>
          <status>L</status>
          <parentTransactionFileHandlingObjectIdentifier>{ $parentId }</parentTransactionFileHandlingObjectIdentifier>
        </stagingTransactionMessage>

let $documentElementName := $content/local-name()
let $dir :=  concat(upper-case(substring($documentElementName,1,1)), substring($documentElementName,2))
let $uri := "/global/com/ffe/" || $dir || "/" || $id || ".xml"
let $collections := ($parentId)

return
    (
    xdmp:log("running process.xqy for " || $URIS_BATCH_REF),
    xdmp:document-insert($uri, $content, (), $collections), 
    $uri 
    )