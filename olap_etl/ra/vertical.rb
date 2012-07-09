# -*- coding: utf-8 -*-
require 'rubygems'
require 'sequel'
require 'rdf'
require 'rdf/rdfxml'
require 'uri'

require './../util.rb'

URI_TABLE_NAME = :uri_tablename
OBSERVATION_INSTANCE_DIR = '/Users/inohiro/github/research/crawler/ra/outputs/'
EV_TIME_DIR = '/Users/inohiro/github/research/crawler/ra/outputs/time/'

GEONAMES_DOMAIN = 'sws.geonames.org'

@db
@counter = 0

def create_uri_tablename
    @db.create_table!( URI_TABLE_NAME, { :engine => 'innodb'} ) do
        primary_key :id
        String :uri
    end
end

def insert_uri_tablename( uri )
    @db[:URI_TABLE_NAME].insret( :uri => uri )
end

def create_table( tablename )
    @db.create_table!( tablename, { :engine => 'innodb' } ) do
        String :subject
        String :predicate
        String :object
        String :value_type
        String :value_type_id
        index :subject
    end
end

def value_divider( object )
end

# 今回は rdf:type が定義されていないので...

def vertical( rdf_type, base_dir )
    #  insert_uri_tablename( rdf_type )
    
    @db = Util.connect_db( { :db => 'ra' } )
    
    table_name = rdf_type.to_sym
    create_table( table_name )
    
    Dir.glob( base_dir + "*.rdf" ) do |f|
        path = "file:" + f.to_s
        puts path
        
        begin
            graph = RDF::RDFXML::Reader.open( f ) do |reader|
                reader.each do |stm|
                    
                    if stm.object.class == RDF::URI
                        type_id = 1
                        datatype = nil
                        object_domain = URI.parse( stm.object.to_s ).host
                        if object_domain == GEONAMES_DOMAIN
                            type_id = 3
                            datatype = 'geonames'
                        end
                        else # Literal
                        type_id = 2
                        if stm.object.has_datatype?
                            datatype = stm.object.datatype
                            else
                            datatype = RDF::XSD.string
                        end
                    end
                    
                    @db[table_name].insert( :subject => stm.subject.to_s,
                                           :predicate => stm.predicate.to_s,
                                           :object => stm.object.to_s,
                                           :value_type_id => type_id.to_i,
                                           :value_type => datatype.to_s )
                end
            end
            rescue => exp
            puts exp.class
            puts exp.message
            puts exp.backtrace
        end
    end
end

def main
    @db = Util.connect_db( { :db => 'ra' } )
    #  create_uri_tablename
    
    =begin
     resources = [ { :type => 'observation_instance', :base_dir => OBSERVATION_INSTANCE_DIR },
     { :type => 'ev_time', :base_dir => EV_TIME_DIR }
     #    ,{ :type => 'ev_place', :base_dir => '' }
     #    ,{ :type => 'scv_dataset', :base_dir => '' }
     ]
     
     resources.each do |r|
     vertical( r[:type], r[:base_dir] )
     end
     =end
    
    puts 'observation_instace start'.upcase
    vertical( "observation_instance", OBSERVATION_INSTANCE_DIR )
    puts 'ev_time start'.upcase
    vertical( "ev_time", EV_TIME_DIR )
end

main
