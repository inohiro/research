# -*- coding: utf-8 -*-

# ARTADE2（http://ja.biolod.org/class/cria227s1i/ARTADE2_Gene_Ver.20100511）から、
# 同一の Tiling Array Analysis Results が持つ Target_gene を抜き出し
# Tiling Array Analysis Result のインスタンス（今回は Leaf（http://ja.biolod.org/class/cria227s403i/Leaf））で
# rdf:type => 'http://biolod.org/class/cria227s8i/Tiling_Array_Analysis_Result' のテーブルに追加する

require 'sequel'
require './../util.rb'
require 'pp'

TARGET_GENE = 'http://biolod.org/property/pria227s1i/Target_gene'

@db
@src

# def insert_info( table, column, type, is_resource )
def insert_info
  begin
    @db[:horizontal_infos].insert( :table_name => 't6_h',
                                  :attribute_name => 'Target_gene',
                                  :data_type => 'String',
                                  :is_resource => true )
  rescue => exp
    puts "!!! unexpected insertion exception !!!".upcase
    puts exp.message
  end
end

def add_column
  begin
    @db.alter_table :t6_h do
      add_column( 'Target_gene'.to_sym, String )
    end
  rescue => exp
    puts "!!! unexpected alter_table exception !!!".upcase
    puts exp.message
  end
end

def main
  @db = Util.connect_db( { :db => 'leaf' } )
  @src = Util.connect_db( { :db => 'artade2_all' } )

  add_column

  @db[:t6_h].all.each do |r|
    subject = r[:subject]
    target = r['Target_gene'.to_sym]
    result = @src[:all_triples].filter( :subject => subject ).filter( :predicate => TARGET_GENE ).each do |c|
      begin
        @db[:t6_h].filter( :subject => subject ).update( 'Target_gene'.to_sym => c[:object] )
      rescue => exp
        puts "!!! unexpected update exception !!!".upcase
        puts exp.message
      end
    end
  end

#  insert_info( table, column, type, is_resource )
  insert_info
end

main
