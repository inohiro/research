require 'rubygems'
require 'sequel'

require 'util.rb'

@db
GEONAMES_FILE = '/Users/inohiro/Downloads/all-geonames-rdf.txt'

def main
  @db = Util.connect_geonames
  table_name = "geonames_rdf".to_sym

  counter = 0
  m = nil
  n = nil

  File::open( GEONAMES_FILE, :encoding => Encoding::UTF_8 ) do |f|
    f.each do |line|
      if counter % 2 == 0
        geonames_id = Util.geonames_id( line )
      else
        @db[table_name].insert( :geonames_id => geonames_id.to_i, :rdfxml => line )
      end
      counter = counter + 1        
    end
  end
end

main
