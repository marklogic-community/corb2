xquery version "1.0-ml";
declare namespace corb = "http://developer.marklogic.com/code/corb";
declare variable $dbName as xs:string external;
declare variable $uriRoot as xs:string external;
declare variable $metricsDocumentStr as xs:string external;
declare variable $collections as xs:string external;
declare variable $DEFAULT_URI_ROOT:="/ServiceMetrics/";

declare private function local:save-metrics-document($svc-name as xs:string,
                                                     $uri as xs:string,
                                                     $metrics-doc as element(),
                                                     $collections)
{
  let $collections := (if($collections) then fn:tokenize($collections, ",") else (), $svc-name)
  let $_ := xdmp:invoke-function(
              function(){xdmp:document-insert($uri, $metrics-doc, xdmp:default-permissions(), $collections)},
              <options  xmlns="xdmp:eval">
               <database>{xdmp:database($dbName)}</database>
               <transaction-mode>update-auto-commit</transaction-mode>
              </options>)
  return $uri
};
let $metrics-document := xdmp:unquote($metricsDocumentStr)/corb:job
(:Job name defaults to job run location:)
let $job-name := if ($metrics-document/corb:name) then $metrics-document/corb:name/text() else $metrics-document/corb:runLocation/text()
let $collections := if($collections and $collections != 'NA') then $collections else ""
let $uri := $metrics-document/corb:metricsDocUri/text()
let $has-end-time := $metrics-document/corb:endTime
let $dateTime := fn:current-dateTime()
let $uri-root := if($uriRoot ne "NA") then $uriRoot else $DEFAULT_URI_ROOT
let $uri-root := if(fn:starts-with($uri-root,"/")) then $uri-root else "/"||$uri-root
let $uri-root := if(fn:ends-with($uri-root,"/")) then $uri-root else $uri-root||"/"
let $orig-uri := $uri
let $job-name := if(fn:starts-with($job-name,"/")) then fn:substring($job-name,2) else $job-name
let $uri := if($uri) then
              if($has-end-time) then $uri(:Job finished so update the root document:)
              else $uri||"/"||xdmp:random()
            else
                $uri-root||"CORB/"||
                $job-name||"/"||
                fn:year-from-dateTime($dateTime)||"/"||
                fn:month-from-dateTime($dateTime)||"/"||
                fn:day-from-dateTime($dateTime)||"/"||
                fn:hours-from-dateTime($dateTime)||"/"||
                fn:minutes-from-dateTime($dateTime)||"/"||
                xdmp:random()||".xml"
 let $orig-uri :=  if($orig-uri) then $orig-uri else $uri
 let $_:= local:save-metrics-document($job-name , $uri, $metrics-document, $collections)
 return $orig-uri
