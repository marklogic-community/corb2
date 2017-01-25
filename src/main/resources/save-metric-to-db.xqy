xquery version "1.0-ml";
  declare variable $db-name as xs:string external;
  declare variable $uri-root as xs:string external;
  declare variable $metrics-document-str as xs:string external;
  declare variable $collections as xs:string external:="";
  declare private function local:save-metrics-document($svc-name as xs:string, $uri-root as xs:string,
                                        $metrics-doc as element(),$collections){
    xdmp:eval(
        '
        declare variable $metrics-doc as element() external;
        declare variable $svc-name as xs:string external;
        declare variable $collections as xs:string external;
        declare variable $uri-root as xs:string external;
         
        let $dateTime := fn:current-dateTime()
        let $uriRoot:=if($uri-root ne "NA") then $uri-root else "/ServiceMetrics/"
        let $uriRoot:=if(fn:starts-with($uriRoot,"/")) then $uriRoot else "/"||$uriRoot
        let $uriRoot:=if(fn:ends-with($uriRoot,"/")) then $uriRoot else $uriRoot||"/"
        let $_:=xdmp:log("uri root ::::::::::"||$uriRoot)
        let $uri := $uriRoot||"CoRB2/"||
                    $svc-name||"/"||
                    fn:year-from-dateTime($dateTime)||"/"||
                    fn:month-from-dateTime($dateTime)||"/"||
                    fn:day-from-dateTime($dateTime)||"/"||
                    fn:hours-from-dateTime($dateTime)||"/"||
                    fn:minutes-from-dateTime($dateTime)||"/"||
                    xdmp:random()||".xml"
        return        
        xdmp:document-insert($uri, $metrics-doc, xdmp:default-permissions(), ((if($collections) then fn:tokenize($collections,",") else () ),$svc-name))
    
        ',
        (
         xs:QName("metrics-doc"),$metrics-doc,
         xs:QName("svc-name"),$svc-name,
         xs:QName("collections"),$collections,
         xs:QName("uri-root"),$uri-root
        ),
        <options xmlns="xdmp:eval">
           <database>{xdmp:database($db-name)}</database>
        </options>
    
    )

};

  let  $metrics-document:=xdmp:unquote($metrics-document-str)/element()
  let $job-name:=if($metrics-document/JobName) then $metrics-document/JobName/text() else $metrics-document/JobLocation/text()
  let $_:=xdmp:log("JOB NAME--------------"||$job-name)
  return local:save-metrics-document($job-name ,$uri-root,$metrics-document,if($collections and $collections != 'NA') then $collections else "")