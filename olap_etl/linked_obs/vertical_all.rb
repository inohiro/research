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

# URI_TABLE_NAME = :uri_tablename
BASE_DIR = '/usr/local/share/data/mouse_mgi_gene_physical_position/xml/'
DATABASE_SCHEMA = 'mouse_mgi_gene'

# BASE_DIR = '/usr/local/share/data/ProteinDataBank/'
# BASE_DIR = '/Users/inohiro/Projects/LinkedSensorData/linkedsensordata/'
# OBSERVATORY_PATH = "file:/Users/inohiro/Projects/rdf_rb/WSFO3_2005_8_26.n3"
# OBSERVATORY_PATH = "file:/Users/inohiro/Projects/rdf_rb/SNHUT_2004_8_11.n3"

@db = nil
@counter = 0
@current_domain = nil
@depth = 0

def initialize_tables
  @db.create_table?( ALL_TRIPLES, { :engine => 'innodb' } ) do
    String :subject
    String :predicate
    String :object
    String :value_type
    String :value_type_id
  end
  @db.create_table?( ALL_RDF_TYPES, { :engine => 'innodb' } ) do
    primary_key :id
    String :uri
  end
end

def value_divider( object )
  if m = /\^\^/.match( object )
    [m.pre_match, m.post_match]
  else
    []
  end
end

def insert( stm, object_alt, type_id, datatype )
  begin
    @db[ALL_TRIPLES].insert( :subject => stm.subject.to_s,
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

require 'pp'
@current_stm

def main
  @db = Util.connect_db( { :db => DATABASE_SCHEMA } )
  initialize_tables # initialization

  Dir.glob( BASE_DIR + "*.xml" ) do |f|

    path = "file:" + f.to_s
    puts "loading...: #{path}"
    tmp_subject =''
    table_id = ''

    begin

      # ToDO: It have to change for input file format

#      graph = RDF::Graph.load( path )
#      graph.each do |stm|

#    graph = RDF::Reader.open( path )
#      graph = RDF::Reader.for( :xml ).open( path )
    graph = RDF::RDFXML::Reader.open( path )
      graph.each do |stm|

      @current_stm = stm

        if stm.predicate == RDF.type
          result = @db[ALL_RDF_TYPES].filter( :uri => stm.object.to_s )
          if result.count < 1
            @db[ALL_RDF_TYPES].insert( :uri => stm.object.to_s )
          end
        end

        if stm.object.class == RDF::Literal # Literal / Resource 判定
          type_id = 2 # Literal

          if stm.object.has_datatype? # 識別可能なデータ型は利用する
            datatype = stm.object.datatype
          else
            # 識別できないデータ型（今回は日時）については，別途分割
            object_alt = nil
            match = value_divider( stm.object.to_s )
            if match.empty?
              datatype = RDF::XSD.string
            else
              object_alt = match[0]
              datatype = match[1]
            end
          end
        elsif stm.object.class == RDF::URI
          type_id = 1 # Resource
          datatype = nil

          # ToDo: create domain list that to process specially
          forward_domain = URI.parse( stm.object.to_s ).host
          if forward_domain == 'sws.geonames.org'
            type_id = 3
            datatype = 'geonames'
          end
        end

        insert( stm, object_alt, type_id, datatype )
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

main
