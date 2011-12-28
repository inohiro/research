# -*- coding: utf-8 -*-

require 'rdf'
require 'rdf/n3'
require 'rdf/rdfxml'

require 'rubygems'
require 'pp'
require 'highline'
require 'sequel'

OBSERVATORY_PATH = "file:/Users/inohiro/Projects/rdf_rb/WSFO3_2005_8_26.n3"
HAS_LOCATION = "http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#hasLocation" # "om-owl:hasLocation"
PARENT_FEATURE = "http://www.geonames.org/ontology#parentFeature"
GEONAMES_NAME = "http://www.geonames.org/ontology#name"

LATITUDE = "http://www.w3.org/2003/01/geo/wgs84_pos#lat"   # 緯度
LONGITUDE = "http://www.w3.org/2003/01/geo/wgs84_pos#long" # 経度
ALTITUDE = "http://www.w3.org/2003/01/geo/wgs84_pos#alt"   # 高度

FOAF_PRIMARY_TOPIC = 'http://xmlns.com/foaf/0.1/primaryTopic'


def get_location( uri )
  puts '=============================='
  puts uri
  RDF::RDFXML::Reader.open( uri + 'about.rdf' ) do |reader|
    reader.each_statement do |stm|
#      puts stm.inspect
      case stm.predicate
      when PARENT_FEATURE
        #        get_location( stm.object.to_s )
      when GEONAMES_NAME
        puts stm.object
      when LATITUDE
      when LONGITUDE
      when ALTITUDE
      end
    end
#    insert( h )
  end
end

def insert( h )

end

def main
  host = HighLine.new.ask ( 'Hostname: ' )
  user = HighLine.new.ask( 'MySQL User: ' )
  passwd = HighLine.new.ask( 'MySQL Password: ' ) { |q| q.echo = '*'}
  dbname = HighLine.new.ask( 'Database Name: ' )

  @DB = Sequel.connect( "mysql://#{user}:#{passwd}@#{host}/#{dbname}", { :encoding => 'utf8' } )

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

  @DB.disconnect
end

main
