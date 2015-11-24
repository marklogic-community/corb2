declare variable $URIS as xs:string external;
let $_ := "hello"

return ("uris_batch_ref_value", xs:integer("1"), "The Selector sends its greetings!  The COLLECTION-NAME is " || $URIS)