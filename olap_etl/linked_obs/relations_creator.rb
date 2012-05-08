# -*- coding: utf-8 -*-
require 'sequel'
require './../util.rb'

require 'rdf'
require 'uri'

@db

RELATION_INFOS_TABLE = :relation_infos

def vertical_explorer( object, table_name )

  puts "object: #{object.to_s}"
  puts "table_name: #{table_name.to_s}"

  table_list = @db[:uri_tablename].all
  table_list.each do |t|
    ft_table_name = ( 't' + t[:id].to_s ).to_sym
    puts "current_table: #{ft_table_name}"
    if table_name != ft_table_name
      ft_all_subjects = @db[ft_table_name].select( :subject ).distinct


      sampling = ft_all_subjects.first
      sampling_domain = URI.parse( sampling[:subject] ).host

      if sampling_domain == URI.parse( object ).host
        ft_all_subjects.each do |ft_s|
          ft_subject = ft_s[:subject]
          if ft_subject == object
            return ft_table_name
          end
        end
      end
    end
  end
  return nil #=> リソースだけどデータセットに含まれていない
end

def horizontal_explorer( object )
  table_list = @db[:uri_tablename].all
  table_list.each do |table|
    current_table = ( 't' +  table[:id].to_s + '_h'  ).to_sym

    sampling = @db[current_table].first
    sampling_domain = URI.parse( sampling[:subject] ).host

    if sampling_domain == URI.parse( object ).host
      @db[current_table].select( :subject ).each do |r|
        subject = r[:subject]
        if subject == object
          return current_table
        end
      end
    end
  end
  return nil
end

def horizontal_main
  @db = Util.connect_db( { :db => 'test' } )

  create_table # create table for save relationship informations

  table_list = @db[:uri_tablename].all
  table_list.each do |table|
    table_name = ( 't' +  table[:id].to_s + '_h' ).to_sym
    puts "Table: #{table_name.to_s}"

    @db[table_name].first.each do |a| # attributes.each
      unless a[0].to_s == 'subject'
        object = a[1]
        if Util.valid_http_uri?( object )
          result = horizontal_explorer( object )
          if result
            puts "save relationship: #{table_name.to_s}.#{a[0].to_s} with #{result}.subject"
            save_relation_info( table_name, a[0], result )
          end
        end
      end
    end
  end
end

# 外部リソースへのリンクは，テーブルだけ作成する（horizontal時）

def create_table
  @db.create_table!( RELATION_INFOS_TABLE, { :engine => 'innodb' } ) do
    column( :table_name, String, :null => false, :index => true )
    column( :column_name, String, :null => false )
    column( :f_table_name, String, :null => false )
    column( :f_column_name, String )
  end
end

def save_relation_info( table_name, column_name, f_table_name, f_column_name = 'subject' )
  @db[RELATION_INFOS_TABLE].insert( :table_name => table_name.to_s,
                                    :column_name => column_name.to_s,
                                    :f_table_name => f_table_name.to_s,
                                    :f_column_name => f_column_name )
end

def vertical_main
  @db = Util.connect_db( { :db => 'test' } )

  table_list = @db[:uri_tablename].all
  table_list.each do |table|
    unless table[:id] == 2 || table[:id] == 9

      table_name = ( 't' + table[:id].to_s ).to_sym
      puts "Table: #{table_name.to_s}"

    a_subject = @db[table_name].select( :subject ).distinct.first
    record = @db[table_name].filter( :subject => a_subject[:subject] )
    record.each do |r|
      if r[:value_type_id] == 1 && r[:predicate] != RDF.type
        # さらに同一ホスト という条件を加える（正しい？
        # まず1つ目だけ比較して，ホストが一緒か確認する．同じなら探す
        object = r[:object]
        result = vertical_explorer( object, table_name )
        if result == nil
          puts "relation not fount"
        else
          puts "result: #{result}"
        end
        puts
      end
    end
  end
end
end

horizontal_main
