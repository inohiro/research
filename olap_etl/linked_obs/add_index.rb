# -*- coding: utf-8 -*-
require 'rubygems'
require 'sequel'
require './../util.rb'

ALL_RDF_TYPES = :all_rdf_types
ALL_TRIPLES = :all_triples
DATABASE_SCHEMA = 'mouse_mgi_gene'

@db

require 'pp'

def main( argv )
  if argv.size < 3
    print_help
  else
    schema_name = argv[0]
    table_name = argv[1]

    if argv.size == 3
      column_name = argv[2]
    else
      column_name = argv[2..argv.size]
    end

    @db = Util.connect_db( { :db => schema_name })

    if @db.table_exists?( table_name.to_sym )
      alter_table( table_name, column_name ) == true ? puts( 'success' ) : puts( 'fail' )
    else
      puts "shema: #{table_name} does not exist"
    end
  end
end

def alter_table( table_name, column_name )
  begin
    @db.alter_table table_name.to_sym do
      if column_name.class == String
        add_index column_name.to_sym
      elsif column_name.class == Array
        column_name.each do |column|
          add_index column.to_sym
        end
      end
    end
  rescue => exp
    puts '!!! unexpected alter_table error !!!'.upcase
    puts exp.message
    puts exp.backtrace
    false
  end
  true
end

def print_help
  puts <<EOS
usage: ruby add_index.rb schema_name table_name column_name [ column_name ]
EOS
end

main( ARGV )
