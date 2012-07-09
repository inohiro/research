# -*- coding: utf-8 -*-
require 'rubygems'
require 'sequel'
require './../util.rb'

@db
@duplicated

def duplicate( table_name )
    puts 'duplicate table'.upcase
    table_name = table_name.to_sym
    
    if @duplicated.index( table_name ) == nil
        puts table_name
        target_table = ( 'neo_' + table_name.to_s ).to_sym
        @db[table_name].each do |r|
            @db[target_table].insert( r )
        end
        @duplicated << table_name
    end
end

def search_rec(  row )
    ref_table = @db[:relation_infos].filter( :table_name => row[:f_table_name] )
    
    if ref_table.count > 0 # dependencies are exists
        search_rec( ref_table.first )
        duplicate( row[:f_table_name] )
        duplicate( row[:table_name] )
        else
        duplicate( row[:f_table_name] )
    end
end

def main
    @duplicated = []
    @db = Util.connect_db( { :db => 'test' } )
    
    tables = []
    @db[:relation_infos].each do |e|
        tables << e[:table_name].to_sym
        search_rec( e )
    end
    
    tables.uniq!
    unduplicated = tables - @duplicated
    
    unduplicated.each do |table|
        duplicate( table )
    end
    pp @duplicated
end

main
