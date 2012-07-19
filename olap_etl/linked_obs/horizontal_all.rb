# -*- coding: utf-8 -*-
require 'rubygems'

require 'sequel'
require 'uri'
require 'pp'

require './../util.rb'

ALL_RDF_TYPES = :all_rdf_types
ALL_TRIPLES = :all_triples

@db

def create_info_table
  @db.create_table!( :horizontal_infos, { :engine => 'innodb' } ) do
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
    pp exp
  end
end

def main
  @db = Util.connect_db( { :db => 'ProteinDataBank_all' } )
  create_info_table

  @db[ALL_RDF_TYPES].each do |rdf_type|
    all_subjects = []
    @db[ALL_TRIPLES].select( :subject ).filter( :object => rdf_type[:uri].to_s ).each {|e| all_subjects << e[:subject] }
    result = @db[ALL_TRIPLES].select( :predicate, :value_type, :value_type_id ).filter( [[ :subject, all_subjects ]] ).distinct

    # { :predicate     => "om-owl#samplingTime",
    #   :value_type    =>"",
    #   :value_type_id => 1 }

    attributes = []
    result.each do |r|
      predicate = r[:predicate].to_s
      value_type = r[:value_type].to_s
      value_id = r[:value_type_id].to_i

      attribute = { } # { :type => datatype, :name => hoge.to_sym }
      data_type = String
      column_name = ''
      is_resource = false

      if m = /\#/.match( predicate ) # URI を解析，カラム名を得る
        column_name = m.post_match
      end

      if value_id == 2 # Literal
        if n = /\#/.match( value_type ) # URI を解析
          if n.post_match =~ /float/
            data_type = Float
          elsif n.post_match =~ /boolean/
            data_type = 'Boolean'
          elsif n.post_match =~ /dateTime/
            data_type = DateTime # 時差の計算をしていない
          elsif n.post_match =~ /integer/
            data_type = Integer
          end
        end
      elsif value_id == 3 # GeoNames
        column_name = 'geonames'
        data_type = String
      elsif value_id == 1 # Resource
        is_resource = true
      end

      attributes << { :type => data_type,
                      :name => column_name,
                      :is_resource => is_resource }
    end
    puts table_name
    h_table_name = table_name.to_s + '_h'
    pp attributes
    p '================================='
    create_table( h_table_name.to_sym, attributes )
    savle_table_info
  end
end

main
