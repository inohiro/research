# -*- coding: utf-8 -*-
require 'rubygems'
require 'sinatra'
require 'rdf'
require 'rdf/rdfxml'
require 'json'

require './../olap_etl/util.rb'

@geonames
@layers

PARENT_FEATURE = "http://www.geonames.org/ontology#parentFeature"
GEONAMES_NAME = "http://www.geonames.org/ontology#name"

get '/' do
  '/:id に GeoNames の ID をリクエストしてください'
end

get '/:id/feature.json' do
  content_type :json

  id = params[:id]
  @layers = []
  @geonames = Util.connect_geonames
  get_location( id )
  JSON.unparse( @layers.reverse )
end

private

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
