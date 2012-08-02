# -*- coding: utf-8 -*-
require 'sequel'
require './../util.rb'

require 'rdf'
require 'uri'

@db

DATABASE_SCHEMA = 'mouse'
ALL_RDF_TYPES = :all_rdf_types
URI_TABLE_NAME = ALL_RDF_TYPES
RELATION_INFOS = :relation_infos
HORIZONTAL_INFOS = :horizontal_infos


def recreate_tables_with_relationships
  puts "recreate horizontal table with relationships".upcase

  foreign_queue = []

  table_list = @db[URI_TABLE_NAME].all
  table_list.each do |table|
    table_name = ( 't' + table[:id].to_s + '_h' )
    puts "Table: #{table_name}"

    attributes = @db[HORIZONTAL_INFOS].filter( :table_name => table_name )
    relations = @db[RELATION_INFOS].filter( :table_name => table_name )

    index_columns = []
    index_columns << :subject

    neo_table_name = ( 'neo_' + table_name ).to_sym

    begin
      @db.create_table!( neo_table_name, { :engine => 'innodb' } ) do
        String :subject,  { :primary_key => true, :null => false }
        attributes.each do |a|
          if a[:is_resource] # リソースだったら
            column( a[:attribute_name], String ) # とりあえず追加しておく
            index_columns << a[:attribute_name].to_sym # インデックスを作る
            if !relations.empty?
              relations.each do |r|
                if r[:column_name] == a[:attribute_name] # 関係があればFKを設定
                  foreign_queue << { :table_name => neo_table_name, # FK はあとで追加する
                                     :column_name => a[:attribute_name],
                                     :foreign_table => ( 'neo_' + r[:f_table_name] ).to_sym,
                                     :foreign_column => :subject }
                end
              end
            end
          else # リソースでなければ，そのまま作る
            column( a[:attribute_name], a[:data_type] == 'String' ? String : a[:data_type] )
          end
        end
        index_columns.each do |c|
          index c
        end
      end
    rescue => exp
      puts '!!! unexpected create table error !!!'.upcase
      puts exp.message
#      puts exp.backtrace
    end
  end

  puts 'add foreign key'.upcase
  table_list.each do |table|
    table_name = ( 'neo_t' + table[:id].to_s + '_h' )

    foreign_queue.each do |q|
      if q[:table_name].to_s == table_name
        @db.alter_table( table_name.to_sym ) do
           result = add_foreign_key( [q[:column_name].to_sym], q[:foreign_table].to_sym, :key => :subject )
          puts "add_foreign_key: #{table_name}.#{q[:column_name]} => #{q[:foreign_table]}.subject"
        end
      end
    end
  end
end


def horizontal_explorer( object, column_name, base_table )
  table_list = @db[URI_TABLE_NAME].all
  table_list.each do |table|
    current_table = ( 't' +  table[:id].to_s + '_h'  ).to_sym

    if current_table != base_table && @db.table_exists?( current_table ) # pruning with table name

      # 現在、サンプリングは1レコードしかやっていないので、
      # 取得したレコードのすべてのカラムが、is null or empty でないかのチェックが必要

      sampling = @db[current_table].first
      begin
        sampling_domain = URI.parse( sampling[:subject] ).host
      rescue => exp
        puts "!!! URI parse error !!!"
        puts sampling[:subject]
        sampleing_domain = nil
      end

      if sampling_domain == URI.parse( object ).host # pruning with URI's host name
        @db[current_table].select( :subject ).each do |r|
          subject = r[:subject]

          one2one = false
          if subject == object # there is a relationship
            result = @db[base_table].filter( column_name => object )
            if result.count >= 2 # 1 to many relationship
              one2one = false
            else # 1 to 1 relationship
              one2one = true
            end
            return current_table, one2one
          end
        end
      end
    end
  end
  return nil
end

def horizontal_main
  create_table # create table for save relationship informations

  table_list = @db[URI_TABLE_NAME].all
  table_list.each do |table|
    table_name = ( 't' +  table[:id].to_s + '_h' ).to_sym
    puts "Table: #{table_name.to_s}"

    if @db.table_exists?( table_name )
      @db[table_name].first.each do |a| # attributes.each
        column_name = a[0].to_s
        object = a[1].to_s
        unless column_name == 'subject'
          if Util.valid_http_uri?( object )
            result, one2one = horizontal_explorer( object, column_name, table_name )
            if result
              puts "save relationship: #{table_name.to_s}.#{column_name} with #{result}.subject"
              save_relation_info( table_name, column_name, result, one2one )
            end
          end
        end
      end
    end
  end
end

# 外部リソースへのリンクは，テーブルだけ作成する（horizontal時）

def create_table
  @db.create_table!( RELATION_INFOS, { :engine => 'innodb' } ) do
    column( :table_name, String, :null => false, :index => true )
    column( :column_name, String, :null => false )
    column( :f_table_name, String, :null => false )
    column( :f_column_name, String )
    column( :is_one2one, 'Boolean' )
  end
end

def save_relation_info( table_name, column_name, f_table_name, f_column_name = 'subject', one2one )
  begin
    @db[RELATION_INFOS].insert( :table_name => table_name.to_s,
                                :column_name => column_name.to_s,
                                :f_table_name => f_table_name.to_s,
                                :f_column_name => f_column_name,
                                :is_one2one => one2one == true ? 1 : 0 )
  rescue => exp
    puts '!!! unexcepted insertion error !!!'.upcase
    puts exp.message
    puts exp.backtrace
    puts <<EOS
:parameters => { :table_name => #{table_name.to_s}, 
                 :column_name => #{column_name.to_s}, 
                 :f_table_name => #{f_table_name.to_s}, 
                 :f_column_name => #{f_column_name.to_s}, 
                 :one2one => #{one2one.to_s} }
EOS
  end
end

def main
  @db = Util.connect_db( { :db => DATABASE_SCHEMA } )
  
  horizontal_main
  recreate_tables_with_relationships
end

main
