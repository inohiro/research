# -*- coding: utf-8 -*-
require 'time'
require './../util.rb'

@db

TIME_INSTANTS = :time_instance
TIME_TABLE = :ev_time_h
TIME_COLUMN = :at

def parse( time )

  if time.class == Time
    time = time.iso8601
  end

  begin
    parsed = Time.iso8601( time )
    layered = Hash.new
    layered.store( :year, parsed.year )
    layered.store( :month, parsed.month )
    layered.store( :day, parsed.day )
    layered.store( :hour, parsed.hour )
    layered.store( :min, parsed.min )
    layered.store( :sec, parsed.sec )
    layered
  rescue => exp
    puts 'parse error...'
    puts exp
    nil
  end
end

def create_table
  @db.create_table!( TIME_INSTANTS, { :engine => 'innodb' } ) do
    String :subject
    Integer :year
    Integer :month
    Integer :day
    Integer :hour
    Integer :min
    Integer :sec
    index :subject
  end
end

def main
  @db = Util.connect_db( { :db => 'ra' } )
  
  create_table
  
  result = @db[TIME_TABLE].all
  result.each do |r|
    layered_time = parse( r[TIME_COLUMN] )
    layered_time.store( :subject, r[:subject] )
    
    if layered_time.size >= 2
      @db[TIME_INSTANTS].insert( layered_time )
    end
  end
end

main
