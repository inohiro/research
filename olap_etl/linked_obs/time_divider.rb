# -*- coding: utf-8 -*-

# Time のパース
#    > require 'time'
#    => true
#    > result = Time.iso8601( "2004-04-01T12:00:00" )
#    => 2004-04-01 12:00:00 0900'
#    > result.year
#    => 2004

require 'time'
require './../util.rb'

@db

TIME_INSTANTS = :time_instants
TIME_TABLE = :t9_h
TIME_COLUM = :inXSDDateTime

def parse( time )
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
  @db = Util.connect_db

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
