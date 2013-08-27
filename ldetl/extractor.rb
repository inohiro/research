# coding: utf-8

require 'pp'
require 'rdf'
require 'rdf/n3'

RDF_PATH = "/Users/inohiro/Documents/linked_sensor_data_rdf"

# genuine_tpts = {
#   'type1' => [
#     [ 'subject1', 'predicate', 'object', 'object_datatype' ],
#     [ 'subject1', 'predicate', 'object', 'object_datatype' ],
#     [ 'subject2', 'predicate', 'object', 'object_datatype' ],
#     [ 'subject2', 'predicate', 'object', 'object_datatype' ],
#   ],
#   'type2' => []
# }
        
# tpts = {
#   'type1' => {
#     'subject1' => [
#       [ 'predicate', 'object', 'object_datatype' ],
#       [ 'predicate', 'object', 'object_datatype' ],
#       [ 'predicate', 'object', 'object_datatype' ]
#     ],
#     'subject2' => [
#       [ 'predicate', 'object', 'object_datatype' ],
#       [ 'predicate', 'object', 'object_datatype' ],
#     ]
#   },
#   'type2' => {},
# }

def main
  tpts = Hash.new { |type, key| hash[key] = Hash.new { |subject, name| subject[name] = Array.new } }

  # iterate XML files
  Dir.glob( File.join( RDF_PATH, '*.n3' ) ) do |file| # TODO: generalize
    puts file
    RDF::N3::Reader.open( file ) do |reader|          # TODO: generalize
      current_rdf_type = nil
      
      reader.each_statement do |statement|
        current_rdf_type = statement.object.to_s if statement.predicate == RDF.type

        # object's datatype detection
        if statement.object.class <= RDF::Literal # Literal
          if statement.object.has_datatype?
            datatype = statement.object.datatype
          else
            # TODO: find datatpe
            datatype = 'RDF::Literal::String'
          end
        elsif statement.object.class == RDF::URI # Resource
          # look up next resource
          datatype = RDF::URI
        end

        tpts.store( current_rdf_type, { statement.subject.to_s => Array.new } ) unless tpts.key? current_rdf_type # fix
        tpts[current_rdf_type].store( statement.subject.to_s, Array.new ) unless tpts[current_rdf_type].key? statement.subject.to_s # fix
        tpts[current_rdf_type][statement.subject.to_s] << [ statement.predicate.to_s, statement.object.to_s, datatype ]
      end
    end
  end
end

main
