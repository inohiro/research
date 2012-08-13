# -*- coding: utf-8 -*-
require 'rubygems'

require 'sequel'
require 'uri'
require 'rdf'
require 'pp'

require './../util.rb'

ALL_RDF_TYPES = :all_rdf_types
ALL_TRIPLES = :all_triples

DATABASE_SCHEMA = 'tair_gene_model'

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
    begin
      @db[:horizontal_infos].insert( :table_name => tablename.to_s,
                                     :attribute_name => a[:column_name].to_s,
                                     :data_type => a[:type].to_s,
                                     :is_resource => a[:is_resource] )
    rescue => exp
      puts "!!! unexpected insertion exception !!!".upcase
      puts exp.message
      puts exp.backtrace
    end
  end
end

def create_table( tablename, attributes )
  index_columns = []
  index_columns << :subject

  begin
    @db.create_table!( tablename, { :engine => 'innodb'} ) do
      String :subject
      attributes.each do |a|
        column( a[:column_name], a[:type] )
        if a[:is_resource] == true
          index_columns << a[:column_name].to_sym
        end
      end
#      index_columns.each do |c|
#        index c
#      end
    end
  rescue => exp
    puts '!!! unexpected insertion exception !!!'.upcase
    puts exp.message
    puts exp.backtrace
  end
end

def detect_attributes
  attributes = []

  tables = @db[:vertical_table_list].all.each do |table|
    table_name = table[:vertical_table_name].to_sym
    puts table_name

    @db[ALL_RDF_TYPES].each do |rdf_type|
      all_subjects = []

      @db[table_name].select( :subject )
                     .filter( :predicate => RDF::type.to_s )
                     .filter( :object => rdf_type[:uri].to_s )
                     .each { |e| all_subjects << e[:subject] }

      result = @db[table_name].select( :predicate, :value_type, :value_type_id )
                              .filter( [[ :subject, all_subjects ]] )
                              .distinct # multi value

      h_table_name = 't' + rdf_type[:id].to_s + '_h'

      unless attributes.any?{ |item| item[:table_name] == h_table_name }
        attributes << { :table_name => h_table_name, :attributes => [] }
      end

      result.each do |r|
        predicate = r[:predicate].to_s
        value_type = r[:value_type].to_s
        value_id = r[:value_type_id].to_i

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

        if attributes.any?{ |item| !item[:attributes].empty? && item[:attributes].any?{ |co_item| co_item[:column_name] == column_name} }
          # already exist => do nothing
        else
          attribute = attributes.find { |item| item[:table_name] == h_table_name }
          attribute[:attributes] << { :type => data_type,
                                      :column_name => column_name,
                                      :is_resource => is_resource }
        end
      end
    end
  end
  attributes
end

def main
  @db = Util.connect_db( { :db => DATABASE_SCHEMA } )
  create_info_table

  attributes = detect_attributes
  attributes.each do |attribute|
    table_name = attribute[:table_name].to_sym
    each_attributes = attribute[:attributes]
    create_table( table_name, each_attributes )
    save_table_info( table_name, each_attributes )
  end
end

main
