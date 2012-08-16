# -*- coding: utf-8 -*-

# 引数で与えられたデータベーススキーマにおいて、
# 引数で与えられた2つのテーブルを統合する
# Join ではなく、データを移行する
# 同一の rdf:type であるが、テーブルが分かれている時などに使う

require 'rubygems'
require 'sequel'

require 'pp'
require './../util.rb'

# DATABASE_SCHEMA = 'analysis_results'
HORIZONTAL_INFOS = :horizontal_infos
@db

def duplicate_data( target_table, tables )
  target_table = target_table.to_sym if target_table.class != Symbol
  tables.each do |table|
    table = table.to_sym if table.class != Symbol
    @db[table].all.each do |r|
      begin
        @db[target_table].insert( r )
      rescue => exp
        puts '!!! unexcepted insertion error !!!'
        puts exp.message
      end
    end
  end
  puts 'data duplication were completed'
end

def create_table( table_name, attributes )
  index_columns = []
  index_columns << :subject

  table_name = table_name.to_sym if table_name.class != Symbol
  begin
    @db.create_table!( table_name, { :engine => 'innodb' } ) do
      String :subject
      attributes.each do |a|
        column( a[:name], a[:type] )
        if a[:is_resource] == true
          index_columns << a[:name].to_sym
        end
      end
    end
  rescue => exp
    puts '!!! unexpected insertion error !!!'.upcase
    puts exp.message
    puts exp.backtrace
  end
  puts "new table: #{table_name.to_s} is created"
  index_columns
end

def create_schema( tables )
  new_schema = []
  tables.each do |table|

    record_infos = @db[HORIZONTAL_INFOS].filter( :table_name => table )
    if record_infos.count == 0
      puts "Table: #{table} does not exist."
      return nil
    else
      if new_schema.size == 0
        record_infos.each do |r|
          new_schema << { :type => Util.detect_data_type( r[:data_type] ), 
                          :name => r[:attribute_name],
                          :is_resource => r[:is_resource] }
        end
      else
        record_infos.each do |r|
          exist_flg = false
          new_schema.each do |attribute|
            if r[:attribute_name] == attribute[:name]
              exist_flg = true
            end
          end
          unless exist_flg
            new_schema << { :type => Util.detect_data_type( r[:data_type] ), 
                            :name => r[:attribute_name],
                            :is_resource => r[:is_resource] }
          end
        end
      end
    end
  end
  puts 'new table schema are created'
  new_schema
end

def print_help
  puts <<EOS
ugase: ruby table_integrator.rb db_name new_table_name table_1 table_2 [ table_n ]
EOS
end

def main( argv )

  if argv.size < 4
    print_help
  else
    db_name = argv[0]
    new_table_name = argv[1]
    if argv.size == 4
      tables = [ argv[2], argv[3] ]
    else
      tables = argv[2..argv.size]
    end

    begin
      @db = Util.connect_db( { :db => db_name } )
      pp @db
    rescue => exp
      puts '!!! unexpected connect db exception !!!'.upcase
      puts exp.message
      return
    end

    new_schema = create_schema( tables )
    pp new_schema
    if new_schema != nil
      index_columns = create_table( new_table_name, new_schema )
      pp index_columns
      duplicate_data( new_table_name, tables )
      Util.add_index( @db, new_table_name, index_columns )
    else
      puts 'error'
    end
  end
end

main( ARGV )
