# -*- coding: utf-8 -*-
require 'rubygems'
require 'sequel'

require './../util.rb'

DATABASE_SCHEMA = 'ARTADE2'
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

  table_list = @db[:uri_tablename].all
  table_list.each do |table|
    table_id = table[:id]
    table_name = ( 't' + table_id.to_s ).to_sym
    h_table_name = ( 't' + table_id.to_s + '_h' ).to_sym

    puts "Table:   #{table_name.to_s}"
    puts "H-Table: #{h_table_name.to_s}"

    if @db.table_exists?( h_table_name )
      all_subjects = @db[table_name].select( :subject ).distinct
      all_subjects.each do |e|
        subject = e[:subject]

        # 同一 Subject のレコードの集合
        records = @db[table_name].filter( :subject => subject )
        
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
        insert( h_table_name, tuple )
      end
    else
      puts "Table: #{h_table_name.to_s} does not exist."
    end
  end
end

main
