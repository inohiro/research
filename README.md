# research

* repository of research

## crawler

* data crawling scripts

## olap_lte

### procidure

* vertical
  * store data to relational table in vertical representation

* horizontal
  *  create tables in horizontal representation

* duplicator
  * copy data from vertical table to horizontal one

* generetadObservation_deleter
  * remove triple that has 'generatedObservation' as a predicate
  * because 'System' has not 'rdf:type'. the scripts hasn't decomposed such RDF triples

* relational_creator
  * detect relationship between tables (horizontal representation)

### other scripts

* geonames_getter
  * get layered stracture of any position from geonames

* time_divider
  * create layered stracture of time that is written with 'xsd:dateTime'

* util
  * utility scripts around connection to databases
  * and others
