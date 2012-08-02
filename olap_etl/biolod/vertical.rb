# -*- coding: utf-8 -*-
require 'rubygems'

require 'rdf'
require 'rdf/n3'
require 'rdf/rdfxml'

require 'sequel'
require 'uri'

require './../util.rb'

DATABASE_SCHEMA = 'mouse_mgi_gene'
URI_TABLE_NAME = :uri_tablename
BASE_DIR = '/usr/local/share/data/mouse_mgi_gene/'

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

def insert( tableid, stm, object_alt, type_id, datatype )
  begin
    @db[tableid].insert( :subject => stm.subject.to_s,
                         :predicate => stm.predicate.to_s,
                         :object => object_alt || stm.object.to_s,
                         :value_type_id => type_id.to_i,
                         :value_type => datatype.to_s )
  rescue => exp
    puts '=== insertion error ==='.upcase
    puts exp.message
    puts exp.backtrace
  end
end


require 'pp'

@current_stm

def main
  @db = Util.connect_db( { :db => DATABASE_SCHEMA } )
  create_uri_tablename # initialize

  Dir.glob( BASE_DIR + "*.nt" ) do |f|

    path = "file:" + f.to_s
    puts path
    tmp_subject =''
    tableid = ''

    begin
#      graph = RDF::Graph.load( path )
#      graph.each do |stm|
#      RDF::Reader.open( path ) do |reader|
#      reader = RDF::Reader.for( :n3 ).open( path )
      reader = RDF::Reader.open( path )
      reader.each do |stm|

        pp stm
        gets

        @current_stm = stm

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
          else
            puts '予期しないデータ構造'
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

        insert( tableid, stm, object_alt, type_id, datatype )

      end
    rescue => ex
      puts '=== something error has occured ==='.upcase
      pp @current_stm
      puts ex.message
      puts ex.backtrace
      puts "COUNTER: #{@counter.to_s}"
      gets
#      puts 'processing continue'.upcase
    end
    puts "COUNTER: #{@counter.to_s}"
    @counter = @counter + 1
  end
end

main
