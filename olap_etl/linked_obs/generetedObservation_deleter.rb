require 'rubygems'
require 'sequel'
require 'pp'

require './../util.rb'

@db

GENERATED_OBSERVATION = "http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#generatedObservation"

def main
    @db = Util.connect_db( { :db => 'test' } )
    table_list = @db[:uri_tablename].all
    table_list.each do |table|
        table_name = ( 't' + table[:id].to_s ).to_sym
        puts "Table : #{table_name.to_s}"
        @db[table_name].filter( :predicate => GENERATED_OBSERVATION ).delete
    end
end

main
