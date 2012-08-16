# -*- coding: utf-8 -*-

require 'sequel'
require './../util.rb'
require 'pp'

@src
@dst

def add_schema_info( table_name, attributes, rdf_type )

  result = @dst[:horizontal_infos].filter( :table_name => table_name.to_s )
  if result.count >= 1
    result.delete
  end

  attributes.each do |a|
    begin
      @dst[:horizontal_infos].insert( :table_name => table_name.to_s,
                                      :attribute_name => a[:column_name].to_s,
                                      :data_type => a[:type].to_s,
                                      :is_resource => a[:is_resource].to_s )
    rescue => exp
      puts "!!! unexpected insertion exception !!!".upcase
      puts exp.message
      puts exp.backtrace
    end
  end
  puts "schema information were added to :horizontal_infos"

  # sampling rdf:type from a resource

  result = @dst[:all_rdf_types].filter( :uri => rdf_type )
  if result.count < 1
    begin
      @dst[:all_rdf_types].insert( :uri => rdf_type )
    rescue => exp
      puts "!!! unexpected insertion exception !!!".upcase
      puts exp.message
      puts exp.backtrace
    end
    puts "rdf type were added to :all_rdf_types"
  end
end

def create_table( table_name, attributes )
  index_columns = []
  index_columns << :subject

  begin
    @dst.create_table!( table_name, { :engine => 'innodb' } ) do
      String :subject
      attributes.each do |a|
        column( a[:column_name], a[:type] )
        if a[:is_resource] == true
          index_columns << a[:column_name].to_sym
        end
      end
    end
  rescue => exp
    puts "!!! unexpected create table exception !!!".upcase
    puts exp.message
    puts exp.backtrace
  end
  puts "new table: #{table_name.to_s} was created"
  index_columns
end

def duplicate_data( src_table, dst_table )
  puts 'data duplication are started'
  src_table = src_table.to_sym
  @src[src_table].each do |r|
    begin
      @dst[dst_table].insert( r )
    rescue => exp
      puts "!!! unexpected insertion exception !!!".upcase
      puts exp.message
      puts exp.backtrace
    end
  end
  puts 'data duplication were completed'
end

def create_schema( src_table )
  columns = @src[:horizontal_infos].filter( :table_name => src_table )
  attributes = []
  columns.each do |r|
    attributes << { :type => Util.detect_data_type( r[:data_type] ),
                    :column_name => r[:attribute_name],
                    :is_resource => r[:is_resource] }
  end
  attributes
end

def print_help
  puts <<EOS
usage: ruby copy_table.db src_db src_table dst_db [table_name]
EOS
end

def main( argv )
  if argv.length < 3
    ptinr_help
  else
    src_db = argv[0]
    src_table = argv[1]
    dst_db = argv[2]
    table_name = ( argv.length == 4 ? argv[3] : "#{src_db}_#{src_table}" ).to_sym

    begin
      @src = Util.connect_db( { :db => src_db } )
      @dst = Util.connect_db( { :db => dst_db } )
    rescue => exp
      puts "!!! unexpected connect db exception !!!".upcase
      puts exp.message
      puts exp.backtrace
      return
    end

    rdf_type = ( @src[src_table.to_sym].select( :type ).first )[:type]

    # src_db からテーブル構造を読み取り、dst_db に作る
    attributes = create_schema( src_table )
    index_columns = create_table( table_name, attributes )
    add_schema_info( table_name, attributes, rdf_type )
    duplicate_data( src_table, table_name )
    Util.add_index( @dst, table_name, index_columns )
  end
end

main( ARGV )
