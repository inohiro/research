require 'rdf'
require 'rdf/n3'
require 'rdf/rdfxml'

# require 'rest-client'
require 'mysql'
require 'pp'
require 'highline'


OBSERVATORY_PATH = "file:/Users/inohiro/Projects/rdf_rb/WSFO3_2005_8_26.n3"
HAS_LOCATION = "http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#hasLocation" # "om-owl:hasLocation"
PARENT_FEATURE = "http://www.geonames.org/ontology#parentFeature"
GEONAMES_NAME = "http://www.geonames.org/ontology#name"

LATITUDE = "http://www.w3.org/2003/01/geo/wgs84_pos#lat"
LONGITUDE = "http://www.w3.org/2003/01/geo/wgs84_pos#long"
ALTITUDE = "http://www.w3.org/2003/01/geo/wgs84_pos#alt"

def get_location( uri )
#      response = RestClient.get( uri << 'about.rdf'  )
  insert( 'hoge' )
  RDF::RDFXML::Reader.open( uri << 'about.rdf' ) do |reader|
    reader.each_statement do |stm|
      if stm.predicate == PARENT_FEATURE
        puts stm.inspect
      elsif stm.predicate == GEONAMES_NAME
        puts stm.inspect
        # LATITUDE
        # LONGITUDE
        # ALTITUDE
      end
    end
  end
end

def insert( h )
  @my.query( 'select * from common_countries order by id' ).each do |r|
    pp r
  end
end

def main
  host = HighLine.new.ask ( 'Hostname: ' )
  user = HighLine.new.ask( 'MySQL User: ' )
  passwd = HighLine.new.ask( 'MySQL Password: ' ) { |q| q.echo = '*'}
  table = HighLine.new.ask( 'Table Name: ' )

  @my = Mysql.connect( host, user, passwd, table )
  @my.charset = 'utf8'

  observatory = RDF::Graph.load( OBSERVATORY_PATH )
  observatory.each_statement do |stm|
    if stm.predicate.to_s == HAS_LOCATION
      begin
        uri = stm.object.to_s
        get_location uri
      rescue => evar
        puts 'HTTP GET failed'
      end
    end
  end

  @my.close
end

main
