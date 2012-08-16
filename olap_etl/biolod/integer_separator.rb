# -*- coding: utf-8 -*-

# 遺伝子の位置情報（:type=>"http://biolod.org/class/crib185u2i/Mouse_MGI_Gene_Physical_Position"）が持つ
# Start, End のそれぞれを、桁毎に分解して、テーブルを作り格納し直す

require 'rubygems'

require 'sequel'
require 'uri'
require 'pp'

require './../util.rb'

ALL_TRIPLES = :all_triples
ALL_RDF_TYPES = :all_rdf_types
DATABASE_SCHEMA = 'mouse'
HORIZONTAL_INFOS = :horizontal_infos

TARGET_TABLE = :t7_h

@db

def separate( i )
  i = i.to_s
  a = []
  i.size.times do |j|
    str = i[j]
    ( i.size - j - 1 ).times do
      str << '0'
    end
    a << str.to_i
  end
  a
end

def recreate_table( recreate_table, attributes )
  index_columns = []
  index_columns << :subject

  begin
    @db.create_table!( recreate_table.to_sym, { :engine => 'innodb' } ) do
      String :subject
      attributes.each do |a|
        column( a[:name].to_sym, a[:type] )
        if a[:is_resource] == true
          index_columns << a[:name].to_sym
        end
      end
      index_columns.each do |c|
        index c
      end
    end
    puts "recreated table: #{recreate_table}"
  rescue => exp
    puts '!!! unexpected create_table error !!!'.upcase
    puts exp.message
  end
end

def data_duplicate( base_table, target_table )
  @db[base_table].all.each do |r|
    tuple = { }

    # { :subject=>"http://biolod.org/Chr8_16226003-16232305/crib185u2rib185u35997i",
    #   :type=>"http://biolod.org/class/crib185u2i/Mouse_MGI_Gene_Physical_Position",
    #   :label=>"Chr8:16226003-16232305 +",
    #   :attributionURL=>
    #   "http://gbrowse.informatics.jax.org/cgi-bin/gbrowse/mouse_current/?ref=8;start=16226003;end=16232305",
    #   :sameAs=>"http://scinets.org/item/crib185u2rib185u35997i",
    #   :NCBIm37=>"http://biolod.org/Chr8/crib100s17rib100s8i",
    #   :strand=>"http://biolod.org/forward/crib99u18rib99s1i",
    #   :end=>16232305,
    #   :start=>16226003}

    r.each_key do |key| # duplicate
      tuple.store( key, r[key] )
    end

    separate( r[:start] ).each_with_index do |place, i|
      tuple.store( "start_l#{i.to_s}".to_sym, place )
    end

    separate( r[:end] ).each_with_index do |place, i|
      tuple.store( "end_l#{i.to_s}".to_sym, place )
    end
    
    insert_data( target_table.to_sym, tuple )
  end
  puts 'data duplication is orver'
end

def insert_data( table_name, tuple )
  begin
    @db[table_name].insert( tuple )
  rescue => exp
    puts '!!! unexpected insertion error !!!'.upcase
    puts exp.message
  end
end

def main
  @db = Util.connect_db( { :db => DATABASE_SCHEMA } )

  max_start = @db[TARGET_TABLE].max( :start )
  max_end = @db[TARGET_TABLE].max( :end )

  max_start_length = max_start.to_s.length
  max_end_length = max_end.to_s.length

  attributes = []

  horizontal_infos = @db[HORIZONTAL_INFOS].filter( :table_name => TARGET_TABLE.to_s )
  horizontal_infos.each do |c|

    # {:id=>37,
    #  :table_name=>"t7_h",
    #  :attribute_name=>"type",
    #  :data_type=>"String",
    #  :is_resource=>true}

    data_type = String
    case c[:data_type] # ToDo: Not enough
    when "String"
      data_type = String
    when "Integer", "Int"
      data_type = Integer
    end

    attributes << { :type => data_type,
                    :name => c[:attribute_name],
                    :is_resource => c[:is_resource] }
  end

  max_start_length.times do |i|
    attributes << { :type => Integer,
                    :name => "start_l#{i.to_s}",
                    :is_resource => false }
  end

  max_end_length.times do |i|
    attributes << { :type => Integer,
                    :name => "end_l#{i.to_s}",
                    :is_resource => false }
  end

  recreate_table = 're_' + TARGET_TABLE.to_s
  recreate_table( recreate_table, attributes )
  data_duplicate( TARGET_TABLE, recreate_table )
end

main
