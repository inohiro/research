# -*- coding: utf-8 -*-
require 'sequel'
require './../util.rb'

require 'pp'
require 'rdf'

require 'uri'

@db

RELATION_INFOS_TABLE = :relation_infos

def horizontal_explorer( object )
    table_list = [ 'observation_instance', 'ev_time' ]
    table_list.each do |table|
        current_table = ( table + '_h' ).to_sym # target of horizontal table
        
        sampling = @db[current_table].first
        sampling_domain = URI.parse( sampling[:subject] ).host
        
        if sampling_domain == URI.parse( object ).host
            @db[current_table].all.each do |r|
                subject = r[:subject]
                if subject == object
                    return current_table
                end
            end
        end
    end
    return nil
end

def recreate_table_with_relationships
    puts "recreate horizontal table with relationships".upcase
    
    foreign_queue = []
    
    #  table_list = @db[:uri_tablename].all
    table_list = [ 'observation_instance', 'ev_time' ]
    table_list.each do |table|
        table_name = ( table + '_h' )
        puts "Table: #{table_name}"
        
        attributes = @db[:horizontal_infos].filter( :table_name => table_name )
        relations = @db[:relation_infos].filter( :table_name => table_name )
        
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
            pp exp
        end
    end
    
    puts 'add foreign key'.upcase
    table_list.each do |table|
        table_name = ( 'neo_' + table + '_h' )
        puts "Table: #{table_name}"
        
        foreign_queue.each do |q|
            if q[:table_name].to_s == table_name
                @db.alter_table( table_name.to_sym ) do
                    result = add_foreign_key( [q[:column_name].to_sym], q[:foreign_table].to_sym, :key => :subject )
                end
            end
        end
    end
end


def horizontal_main
    @db = Util.connect_db( { :db => 'ra' } )
    
    create_table # create table for save relationship informations
    
    table_list = [ 'observation_instance', 'ev_time' ]
    table_list.each do |table|
        table_name = ( table + '_h' ).to_sym
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

horizontal_main
recreate_table_with_relationships

