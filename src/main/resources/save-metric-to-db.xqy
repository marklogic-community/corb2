xquery version "1.0-ml";
declare namespace corb2="http://marklogic.github.io/corb/";
  declare variable $dbName as xs:string external;
  declare variable $uriRoot as xs:string external;
  declare variable $metricsDocumentStr as xs:string external;
  declare variable $collections as xs:string external;
declare private function local:save-metrics-document($svc-name as xs:string, 
                                                        $uriRoot as xs:string,
                                                        $metrics-doc as element(),
                                                        $collections){
        let $dateTime := fn:current-dateTime()
        let $uri-root:=if($uriRoot ne "NA") then $uriRoot else "/ServiceMetrics/"
        let $uri-root:=if(fn:starts-with($uri-root,"/")) then $uri-root else "/"||$uri-root
        let $uri-root:=if(fn:ends-with($uri-root,"/")) then $uri-root else $uri-root||"/"
        let $uri := $uri-root||"CoRB2/"||
                    $svc-name||"/"||
                    fn:year-from-dateTime($dateTime)||"/"||
                    fn:month-from-dateTime($dateTime)||"/"||
                    fn:day-from-dateTime($dateTime)||"/"||
                    fn:hours-from-dateTime($dateTime)||"/"||
                    fn:minutes-from-dateTime($dateTime)||"/"||
                    xdmp:random()||".xml"
        let $collections:=((if($collections) then fn:tokenize($collections,",") else () ),$svc-name)
       return        
        xdmp:invoke-function( function(){xdmp:document-insert($uri, $metrics-doc, xdmp:default-permissions(), $collections)},     
        <options  xmlns="xdmp:eval">
           <database>{xdmp:database($dbName)}</database>
           <transaction-mode>update-auto-commit</transaction-mode>
        </options>)

};
let  $metrics-document:=xdmp:unquote($metricsDocumentStr)/corb2:job
  let $job-name:=if($metrics-document/corb2:name) then $metrics-document/corb2:name/text() else $metrics-document/corb2:runLocation/text()
  let $collections:=if($collections and $collections != 'NA') then $collections else ""
  return local:save-metrics-document($job-name ,$uriRoot,$metrics-document,$collections)