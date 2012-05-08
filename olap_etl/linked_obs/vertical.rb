# -*- coding: utf-8 -*-
require 'rubygems'

require 'rdf'
require 'rdf/n3'
require 'rdf/rdfxml'

require 'sequel'
require 'uri'
require 'pp'

require './../util.rb'

URI_TABLE_NAME = :uri_tablename
BASE_DIR = '/Users/inohiro/Projects/LinkedSensorData/bill/rdf/'
# BASE_DIR = '/Users/inohiro/Projects/LinkedSensorData/linkedsensordata/'
# OBSERVATORY_PATH = "file:/Users/inohiro/Projects/rdf_rb/WSFO3_2005_8_26.n3"
# OBSERVATORY_PATH = "file:/Users/inohiro/Projects/rdf_rb/SNHUT_2004_8_11.n3"

@db = nil
@counter = 0
@current_domain = nil
@depth = 0

def create_uri_tablename
  @db.create_table!( URI_TABLE_NAME, { :engine => 'innodb' } ) do
    primary_key :id
    String :uri
  end
end

def create_table( tablename )
#  @db.create_table( tablename, { :engine => 'myisam' } ) do
  @db.create_table( tablename, { :engine => 'innodb' } ) do
    String :subject
    String :predicate
    String :object
    String :value_type
    Integer :value_type_id
    index :subject
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
  @db[tableid].insert( :subject => stm.subject.to_s,
                       :predicate => stm.predicate.to_s,
                       :object => object_alt || stm.object.to_s,
                       :value_type_id => type_id.to_i,
                       :value_type => datatype.to_s )
end

def main
  @db = Util.connect_db
  create_uri_tablename # initialize

  Dir.glob( BASE_DIR + "*.n3" ) do |f|

    path = "file:" + f.to_s
    tmp_subject =''
    tableid = ''

    begin
      graph = RDF::Graph.load( path )
      graph.each do |stm|
        if tmp_subject != stm.subject.to_s # predicate の変わり目
          tmp_subject = stm.subject.to_s

          reuslt = nil
          if stm.predicate == RDF.type
            # predicate が rdf:type のとき，テーブルを確認，作る
            # Linked Sensor/Observation Data はこれでいいけど，
            # 本来は全てのトリプルについて，predicate が rdf:type でないか
            # 見るべき（'a' を記述する順番は保証されてないはず）
            tablename = stm.object.to_s.gsub(/\s+/, "")
            result = @db[URI_TABLE_NAME].filter( :uri => tablename )

            if result.count < 1 # 既にあるか確認する，なければ作る
              @db[URI_TABLE_NAME].insert( :uri => tablename )
              tablename_id = @db[URI_TABLE_NAME].order( :id ).last
              table_symbol = ( 't' + tablename_id[:id].to_s ).to_sym
              create_table( table_symbol )
            end

            # table-id をゲットして，テーブルを見つける
            tableid = ( 't' + result[:id][:id].to_s ).to_sym
#          else
#            puts '予期しないデータ構造'
          end
        end
        # Literal / Resource 判定
        if stm.object.class == RDF::Literal
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

          # 同一ドメイン内のリソースを，再帰的に取得 => 今回はやらない
          #      domain = URI.parse( stm.subject.to_s ).host
          forward_domain = URI.parse( stm.object.to_s ).host

          if forward_domain == 'sws.geonames.org'
            type_id = 3
            datatype = 'geonames'
          end
        end

        insert( stm, object_alt, type_id, datatype )

#        puts "Predicate:    #{stm.predicate.to_s}"
#        puts "Object:       #{stm.object.to_s}"
      end
    rescue => ex
      puts 'something error occured'.upcase
      pp ex
      puts "COUNTER: #{@counter.to_s}"
      puts 'processing continue'.upcase
    end
    puts "COUNTER: #{@counter.to_s}"
    @counter = @counter + 1
  end
end

main
