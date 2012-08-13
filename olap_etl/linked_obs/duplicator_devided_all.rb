# -*- coding: utf-8 -*-
require 'rubygems'
require 'sequel'
require 'pp'
require 'rdf'

require './../util.rb'

ALL_RDF_TYPES = :all_rdf_types
ALL_TRIPLES = :all_triples
DATABASE_SCHEMA = 'tair_gene_model'
@db

def insert( table_name, tuple )
  begin
    @db[table_name].insert( tuple )
  rescue => exp
    puts '!!! unexpected insertion error !!!'.upcase
    puts exp.message
    puts exp.backtrace
  end
end

def main

  # データを移行する
  # subject が同じレコードを取得，hash を作ってデータを挿入

  @db = Util.connect_db( { :db => DATABASE_SCHEMA } )

  tables = @db[:vertical_table_list].all.each do |table|
    target_table = table[:vertical_table_name].to_sym
    
    @db[ALL_RDF_TYPES].each do |rdf_type|
      puts "rdf_type: #{rdf_type}"
      all_subjects = []

      table_name = ( 't' + rdf_type[:id].to_s + '_h' ).to_sym

      if @db.table_exists?( table_name )
        @db[target_table].select( :subject )
          .filter( :predicate => RDF::type.to_s )
          .filter( :object => rdf_type[:uri].to_s )
          .each {|e| all_subjects << e }

        all_subjects.each do |e|
          subject = e[:subject]
          records = @db[target_table].filter( :subject => subject )
          
          tuple = Hash.new
          tuple.store( 'subject', subject )

          records.each do |r|

            column_name = ''
            data_type = 'string'
            real_value = nil
            
            predicate = r[:predicate]
            object = r[:object]
            value_type = r[:value_type]
            value_id = r[:value_type_id].to_i
            
            # データ型によって，キャストが必要（Float, Boolean, integer）
            if m = /\#/.match( value_type )
              if m.post_match =~ /float/
                real_value = object.to_f
              elsif m.post_match =~ /boolean/
                real_value = object == 'true' ? true : false
              elsif m.post_match =~ /integer/
                real_value =~ /integer/
                real_value = object.to_i
              else
                real_value = object
              end
            else
              real_value = object
            end
            
            column_name = Util.get_column_name( predicate )
            
            if value_id == 3
              column_name = 'geonames'
            end
            
            tuple.store( column_name, real_value )
          end
          insert( table_name, tuple )
        end
      else
        puts "Table: #{table_name.to_s} does not exist."
      end
    end
  end
end

main
