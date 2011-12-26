require 'rdf'
require 'rdf/n3'
require 'rdf/rdfxml'

require 'rest-client'
require 'pp'


def get_location( uri )
end

OBSERVATORY_PATH = "file:/Users/inohiro/Projects/rdf_rb/WSFO3_2005_8_26.n3"

graph = RDF::Graph.load( OBSERVATORY_PATH )

graph.each_statement do |stm|
  if stm.predicate.to_s == "http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#hasLocation" # "om-owl:hasLocation"
    begin
      uri = stm.object.to_s
      puts stm.object
#      response = RestClient.get( uri << 'about.rdf'  )
#      RDF::RDFXML::Reader.open( response ) do |reader|
      RDF::RDFXML::Reader.open( uri << 'about.rdf' ) do |reader| # RDFXML::Reader.open can open URI
        reader.each_statement do |statement|
          puts statement.inspect
#          puts '\t' << statement.inspect
        end
      end
      #    response = RestClient.get( stm.predicate.to_s )
    rescue => evar
      puts 'GET failed'
    end
  end
end
