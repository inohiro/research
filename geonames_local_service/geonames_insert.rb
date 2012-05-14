require 'rubygems'
require 'sequel'

require './../olap_etl/util.rb'

@db
GEONAMES_FILE = 'all-geonames-rdf.txt'
GEONAMES_TABLE_NAME = "geonames_rdf"

#
#== main
#
# Insert GeoNames RDF dump to MySQL (as an id and xml each point)
# You can get rdf dump from here: http://www.geonames.org/ontology/documentation.htm
# You have to set file path to GEONAMES_FILE
#
def main
  @db = Util.connect_geonames
  table_name = GEONAMES_TABLE_NAME.to_sym

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
