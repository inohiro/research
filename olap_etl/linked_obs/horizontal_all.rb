# -*- coding: utf-8 -*-
require 'rubygems'

require 'sequel'
require 'uri'
require 'rdf'
require 'pp'

require './../util.rb'

ALL_RDF_TYPES = :all_rdf_types
ALL_TRIPLES = :all_triples

DATABASE_SCHEMA = 'mouse'

@db

def create_info_table
  @db.create_table!( :horizontal_infos, { :engine => 'innodb' } ) do
    primary_key :id
    String :table_name
    String :attribute_name
    String :data_type
    Boolean :is_resource
  end
end

def save_table_info( tablename, attributes )
  attributes.each do |a|
    @db[:horizontal_infos].insert( :table_name => tablename,
                                   :attribute_name => a[:name].to_s,
                                   :data_type => a[:type].to_s,
                                   :is_resource => a[:is_resource] )
  end
end

def create_table( tablename, attributes )
  index_columns = []
  index_columns << :subject

  begin
    @db.create_table!( tablename, { :engine => 'innodb'} ) do
      String :subject
      attributes.each do |a|
        column( a[:name], a[:type] )
        if a[:is_resource] == true
          index_columns << a[:name].to_sym
        end
      end
      index_columns.each do |c|
        index c
      end
    end
  rescue => exp
    puts '!!! unexpected insertion error !!!'.upcase
    puts exp.message
    puts exp.backtrace
  end
end

def main
  @db = Util.connect_db( { :db => DATABASE_SCHEMA } )
  create_info_table

  @db[ALL_RDF_TYPES].each do |rdf_type|
    all_subjects = []

    @db[ALL_TRIPLES].select( :subject )
                    .filter( :predicate => RDF::type.to_s )
                    .filter( :object => rdf_type[:uri].to_s )
                    .each {|e| all_subjects << e[:subject] }

    result = @db[ALL_TRIPLES].select( :predicate, :value_type, :value_type_id )
                             .filter( [[ :subject, all_subjects ]] )
                             .distinct

=begin # for too much WHERE conditions. but too slow...
    result = []
    all_subjects.each do |subject|
      # それぞれの Subject が持つ Predicate を取得し、論理積をとる
      query = @db[ALL_TRIPLES].select( :predicate, :value_type, :value_type_id )
                              .filter( :subject => subject )

      query.each do |r|
        result = result | [r]
      end
    end
=end

    table_name = 't' + rdf_type[:id].to_s + '_h'

    attributes = []
    result.each do |r|
      predicate = r[:predicate].to_s
      value_type = r[:value_type].to_s
      value_id = r[:value_type_id].to_i

      attribute = { } # { :type => datatype, :name => hoge.to_sym }
      data_type = String
      column_name = ''
      is_resource = false

      column_name = Util.get_column_name( predicate ) # estimate column name from predicate

      if value_id == 2 # Literal

        data_type = Util.detect_data_type( value_type )

#      elsif value_id == 3 # GeoNames
#        column_name = 'geonames'
#        data_type = String

      elsif value_id == 1 # Resource
        is_resource = true
      end

      attributes << { :type => data_type,
                      :name => column_name,
                      :is_resource => is_resource }
    end

    puts table_name
    pp attributes
    p '================================='
    create_table( table_name.to_sym, attributes )
    save_table_info( table_name, attributes )
  end
end

main
