# -*- coding: utf-8 -*-
require 'rubygems'
require 'sequel'

require 'rdf'
require 'rdf/rdfxml'

require './../util.rb'

@db
@layers

GEONAMES_FEATURES = :geonames_features
PARENT_FEATURE = "http://www.geonames.org/ontology#parentFeature"
GEONAMES_NAME = "http://www.geonames.org/ontology#name"

#
#== get_location
#
# geonames_id を参照して，地球まで見に行った結果を配列にして返す
#
def get_location( geonames_id )
  result = @geonames[:geonames_rdf].filter( :geonames_id => geonames_id )
  result.each do |r|
    RDF::RDFXML::Reader.new( r[:rdfxml].to_s ) do |reader|
      reader.each do |stm|
        case stm.predicate
        when PARENT_FEATURE
          get_location( Util.geonames_id( stm.object ) )
        when GEONAMES_NAME
          @layers << stm.object
        end
      end
    end
  end
end

def create_table
  @db.create_table!( GEONAMES_FEATURES, { :engine => 'innodb' } ) do
    String :subject
    String :layer_1
    String :layer_2
    String :layer_3
    String :layer_4
    String :layer_5
    String :layer_6
    String :layer_7
    String :layer_8 # 予備
    index :subject
  end
end

def insert_info( subject, layers )
  tuple = Hash.new
  tuple.store( :subject, subject )
  layers.each_with_index do |l,i|
    tuple.store( ( 'layer_' + ( i + 1 ).to_s ).to_sym, l.to_s )
  end

  @db[GEONAMES_FEATURES].insert( tuple )
end

def main
  @db = Util.connect_db
  @geonames = Util.connect_geonames

  create_table

  result = @db[:t3].filter( :value_type_id => 3 )
  result.each do |r|
    @layers = []
    get_location( Util.geonames_id( r[:object] ) )
    insert_info( r[:object], @layers.reverse )
  end
end

main
