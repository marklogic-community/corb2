(:
 : Copyright (c)2005-2007 Mark Logic Corporation
 :
 : Licensed under the Apache License, Version 2.0 (the "License");
 : you may not use this file except in compliance with the License.
 : You may obtain a copy of the License at
 :
 : http://www.apache.org/licenses/LICENSE-2.0
 :
 : Unless required by applicable law or agreed to in writing, software
 : distributed under the License is distributed on an "AS IS" BASIS,
 : WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 : See the License for the specific language governing permissions and
 : limitations under the License.
 :
 : The use of the Apache License does not indicate that this project is
 : affiliated with the Apache Software Foundation.
 :)

define variable $URI as xs:string external

define variable $MONTHS as xs:string+ {
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec' }

define function lead-zero($i as xs:string, $len as xs:integer)
 as xs:string {
  concat(string-pad("0", $len - string-length(string($i))), string($i))
}

for $date in doc($URI)//Year/..[ not(@iso8601) ]
let $year := string($date/Year)
let $month := (string($date/Month), '1')[. ne ''][1]
let $month := lead-zero(
  if ($month castable as xs:integer) then $month
  else string(index-of($MONTHS, $month)), 2)
let $day := lead-zero((string($date/Day), '1')[. ne ''][1], 2)
let $value := xs:date(string-join(($year, $month, $day), '-'))
return xdmp:node-insert-child($date, attribute iso8601 { $value })
,
(: by convention, we return the output-uri :)
$URI

(: medline-iso8601.xqy :)