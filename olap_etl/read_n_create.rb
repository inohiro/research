# -*- coding: utf-8 -*-
require 'find'
require 'rubygems'

require 'rdf'
require 'rdf/n3'
require 'rdf/rdfxml'
require 'sequel'

require 'pp'

BASEDIR = '/Users/inohiro/Projects/LinkedSensorData/bill/rdf/'
RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'

@db

def connect_db
#  Sequel.connect( "mysql://#{user}:#{passwd}@#{host}/#{dbname}",
  Sequel::Model.plugin :schema

end

def create_table( tablename )
  @db.create_table tablename do
    String :subject
    String :predicate
    String :object
    String :value_type
    Integer :value_type_id
  end
end

# http://sequel.rubyforge.org/rdoc-plugins/classes/Sequel/Plugins/Schema/ClassMethods.html

# table_create?( :table_name )
# table_exist?( :table_name )

def main

  @db = connect_db
  counter = 0

  Dir.glob( BASEDIR + "*.n3" ) do |f|
    path = "file:" + f.to_s

    begin
      obs = RDF::Graph.load( path )
      obs.each_statement do |stm|
        if stm.predicate == RDF_TYPE
          # predicate が rdf:type のとき，テーブルを確認，作る
          tablename = stm.object.to_s.gsub(/\s+/, "")
          result = @db[:uri_tablename].filter( :uri => tablename )

          if result.count < 1 # 既にあるか確認する，なければ作る
            @db[:uri_tablename].insert( :uri => tablename )
            tablename_id = @db[:uri_tablename].order( :id ).last
            table_symbol = ( 't' + tablename_id[:id].to_s ).to_sym
            create_table( table_symbol )
#            @db.create_table!( ( 't' + tablename_id[:id].to_s ).to_sym ){String :subject, String :predicate, String :object, Integer :value_type_id, String :value_type}
          end

          # table-id をゲットして，テーブルを見つける
          tableid = ( 't' + result[:id][:id].to_s ).to_sym
#          tableid = table_symbol
          type_id = 0

          datatype = RDF::XSD.string
          
          # Literal / Resource
          if stm.object.class == RDF::Literal
            type_id = 2 # Literal
            if stm.object.has_datatype?
              datatype = stm.object.datatype
            else
              datatype = RDF::XSD.string
            end
          elsif stm.object.class == RDF::URI
            type_id = 1 # Resource
            unless stm.object =~ /ssw/ # Object の rdf:type を探しに行く
              graph = RDF::Graph.load( stm.object )
              graph.each_statement do |g|
              if g.predicate == RDF.type
                datatype = g.object
               end
            end
          end
          
            p '--------------------------'
            pp stm.subject
            pp stm.predicate
            pp stm.object
            pp type_id
            pp datatype
            pp tableid
            
            @db[tableid].insert( :subject => stm.subject.to_s, 
                                 :predicate => stm.predicate.to_s, 
                                 :object => stm.object.to_s, 
                                 :value_type_id => type_id.to_i, 
                                 :value_type => datatype.to_s )
            pp result 
            puts "#### COUNTER: #{counter}"
          end
        end
      end
      counter = counter + 1
    rescue => ever
#      puts 'occerd some error...'
    end
#    exit( 10 )
  end
  puts 'end'
end

main
