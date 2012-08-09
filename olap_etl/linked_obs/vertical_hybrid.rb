# -*- coding: utf-8 -*-
require 'rubygems'

require 'rdf'
require 'rdf/n3'
require 'rdf/rdfxml'

require 'sequel'
require 'uri'
require 'pp'

require './../util.rb'

ALL_TRIPLES = :all_triples
ALL_RDF_TYPES = :all_rdf_types
TRIPLE_COUNTER_LIMIT = 10000
VERTICAL_TABLE_LIST = :vertical_table_list

BASE_DIR = '/home/inohiro/Data/artade2/tiling-array_analysis_result/'
DATABASE_SCHEMA = 'artade2_devide'

@db = nil
@counter = 0
@current_domain = nil
@depth = 0
@current_stm

def create_vertical_table( table_name )
  @db.create_table?( table_name, { :engine => 'innodb' } ) do
    String :subject
    String :predicate
    String :object
    String :value_type
    String :value_type_id
  end
end

def initialize_tables( table_name )
  create_vertical_table( table_name )
  @db.create_table?( ALL_RDF_TYPES, { :engine => 'innodb' } ) do
    primary_key :id
    String :uri
  end
  @db.create_table?( VERTICAL_TABLE_LIST, { :engine => 'innodb' } ) do
    primary_key :id
    String :vertical_table_name
  end
  @db[VERTICAL_TABLE_LIST].insert( :vertical_table_name => table_name.to_s )
end

def value_divider( object )
  if m = /\^\^/.match( object )
    [m.pre_match, m.post_match]
  else
    []
  end
end

def insert( stm, object_alt, type_id, datatype, table_name )
  begin
    @db[table_name].insert( :subject => stm.subject.to_s,
                             :predicate => stm.predicate.to_s,
                             :object => object_alt || stm.object.to_s,
                             :value_type_id => type_id.to_i,
                             :value_type => datatype.to_s )
  rescue => ex
    puts '!!! unexpected insertion error !!!'.upcase
    puts ex.class
    puts ex.message
    puts ex.backtrace
    puts "parameters: { stm => #{stm}, object_alt => #{object_alt}, type_id => #{type_id}, datatype => #{datatype}, table_id => #{table_id} }"
  end
end

def print_help
end

def main( argv )
  @db = Util.connect_db( { :db => DATABASE_SCHEMA } )

  triple_counter = 0
  table_number = 0
  table_name = "vertical_all_0".to_sym

  initialize_tables( table_name )
  
  Dir.glob( BASE_DIR + "*.xml" ) do |f|
    path = "file:" + f.to_s
    puts "loading...: #{path}"

    tmp_subject = ''
    table_id = ''

    begin

      reader = RDF::RDFXML::Reader.open( path )
      reader.each do |stm|
        
        @current_stm = stm
        
        if stm.predicate == RDF.type
          result = @db[ALL_RDF_TYPES].filter( :uri => stm.object.to_s )
          if result.count < 1
            @db[ALL_RDF_TYPES].insert( :uri => stm.object.to_s )
          end
        end
        
        if stm.object.class <= RDF::Literal # Literal / Resource 判定
          type_id = 2 # Literal
          
          if stm.object.has_datatype? # 識別可能なデータ型は利用する
            data_type = stm.object.class.to_s # like RDF::Literal::String
          else
            # 識別できないデータ型（今回は日時）については，別途分割
            object_alt = nil
            match = value_divider( stm.object.to_s )
            if match.empty?
              data_type = 'RDF::Literal::String'
            else
              object_alt = match[0]
              data_type = match[1]
            end
          end
        elsif stm.object.class == RDF::URI

          type_id = 1 # Resource
          data_type = nil

          # ToDo: create domain list that to process specially
          forward_domain = URI.parse( stm.object.to_s ).host
          if forward_domain == 'sws.geonames.org'
            type_id = 3
            data_type = 'geonames'
          end
        end

        if triple_counter >= TRIPLE_COUNTER_LIMIT # replace to new table
          triple_counter = 0
          table_number += 1
          table_name = ( "vertical_all_" + table_number.to_s ).to_sym
          create_vertical_table( table_name )
          @db[VERTICAL_TABLE_LIST].insert( :vertical_table_name => table_name.to_s )
        end
        
        insert( stm, object_alt, type_id, data_type, table_name )
        triple_counter += 1
      end

    rescue => ex
      puts '!!! something error has occured !!!'.upcase
      pp @current_stm
      puts ex.message
      puts "CURRENT COUNTER: #{@counter.to_s}"
    end
    puts "COUNTER: #{@counter.to_s}"
    @counter = @counter + 1
  end
end

main( ARGV )
