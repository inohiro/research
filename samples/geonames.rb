require 'linkeddata'
require 'rdf'
require 'sparql'

base = 'http://sws.geonames.org/5606064/'
rdf = base << "about.rdf"

graph = RDF::Graph.load( rdf )
puts graph.size
puts graph.to_s

