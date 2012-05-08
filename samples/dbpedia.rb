require 'rdf'
require 'linkeddata'
require 'sparql'

graph = RDF::Graph.load("http://dbpedia.org/resource/Elvis_Presley")
puts graph.size
puts graph.to_s

require 'pp'
pp graph

query = RDF::Query.new({  :person => { 
    RDF::URI("http://dbpedia.org/ontology/birthDate") => :birthDate,
    RDF::URI("http://dbpedia.org/ontology/deathDate") => :deathDate
                         }
                       })


results = query.execute(graph)

puts results.first[:birthDate].to_s
