# -*- coding: utf-8 -*-

require 'sequel'
require './../util.rb'
require 'pp'

TARGET_GENE = 'http://biolod.org/property/pria227s1i/Target_gene'

@db
@src

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
  
end

main
