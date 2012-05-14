# -*- coding: utf-8 -*-
require 'rubygems'
require 'sequel'
require 'rest_client'
require 'json'

require './../olap_etl/util.rb'

@db
GEONAMES_FEATURES = :geonames_features

def create_table
  @db.create_table! GEONAMES_FEATURES do
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

  create_table

  result = @db[:t11].filter( :value_type_id => 3 )
  result.each do |r|
    geonames_id = Util.geonames_id( r[:object] )
    json = RestClient.get( "http://192.168.0.195:4567/#{geonames_id}/feature.json" )
    insert_info( r[:object], JSON.parse( json ) )
  end
end

main
