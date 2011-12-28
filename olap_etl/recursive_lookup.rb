require 'rdf'
require 'rdf/n3'
require 'rdf/rdfxml'

require 'pp'

PARENT_FEATURE = "http://www.geonames.org/ontology#parentFeature"
GEONAMES_NAME = "http://www.geonames.org/ontology#name"

def get_location( uri )
  puts '=============================='
  puts uri
  RDF::RDFXML::Reader.open( uri + 'about.rdf' ) do |reader|
    reader.each_statement do |stm|
#      puts stm.inspect
      case stm.predicate
      when PARENT_FEATURE
        get_location( stm.object.to_s )
      when GEONAMES_NAME
        puts stm.object
      end
    end
  end
end

def main
  point = ARGV[0]
  base_uri = 'http://sws.geonames.org/'

  uri = base_uri + point + '/'
  get_location( uri )
end

main
