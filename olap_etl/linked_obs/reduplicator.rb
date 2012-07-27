# -*- coding: utf-8 -*-
require 'rubygems'
require 'sequel'
require './../util.rb'
require 'pp'

DATABASE_SCHEMA = 'mouse'
RELATION_INFOS = :relation_infos

@db
@duplicated

def duplicate( table_name )
  puts 'duplicate table'.upcase
  table_name = table_name.to_sym

  if @db.table_exists?( table_name )
    if @duplicated.index( table_name ) == nil
      puts table_name
      target_table = ( 'neo_' + table_name.to_s ).to_sym
      @db[table_name].each do |r|
        result = @db[target_table].filter( :subject => r[:subject] )
        if result.count < 1
          begin
            @db[target_table].insert( r )
          rescue => exp
            puts '!!! unexpected insertion error !!!'
            puts exp.message
            puts "table_name: #{table_name.to_s}"
          end
        end
      @duplicated << table_name
      end
    end
  end
  puts "#{table_name.to_s} completed"
end

def search_rec( row )
  ref_table = @db[RELATION_INFOS].filter( :table_name => row[:f_table_name] )
  
  if ref_table.count > 0 # dependencies are exists
    ref_table.each do |table|
      search_rec( table )
    end
    duplicate( row[:f_table_name] )
  else
    duplicate( row[:f_table_name] )
  end
end

def main
  @duplicated = []
  @db = Util.connect_db( { :db => DATABASE_SCHEMA } )
  
  tables = []
  @db[RELATION_INFOS].each do |e|
    tables << e[:table_name].to_sym
    search_rec( e )
  end
  
  tables.uniq!
  unduplicated = tables - @duplicated
  
  unduplicated.each do |table|
    duplicate( table )
  end
end

main
