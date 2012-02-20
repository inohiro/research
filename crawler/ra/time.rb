require 'rest_client'
require 'rdf'
require 'rdf/rdfxml'

require 'pp'

INPUT_DIR = './outputs/'
TIME_DIR = INPUT_DIR + 'time/'
TIME = "http://purl.org/NET/c4dm/event.owl#time"

def main

  unless File.exists?( INPUT_DIR )
    return 1
  end

  unless File.exists?( TIME_DIR )
    Dir.mkdir( TIME_DIR )
  end

  i = 1
  j = 1

  Dir.glob( INPUT_DIR + '*.rdf' ) do |f|
    puts f
    RDF::RDFXML::Reader.open( f ) do |reader|
      reader.each do |stm|
        if stm.predicate.to_s == TIME
          begin
            result = RestClient.get( stm.object.to_s, :timeout => 10 )
            file_path = "#{TIME_DIR}#{i.to_s}_#{j.to_s}.rdf"
            open( file_path, 'w' ) do |f|
              f.write( result )
            end
          rescue => exp
            pp exp
          end
          j = j + 1
          sleep( 3 )
        end
      end
    end
    j = 1
    i = i + 1
  end
end

main
