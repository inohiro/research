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

#
# == Prevent inserting duplicated predicate (property table)
# === remove object also
#
# subject_reduced_tpts = {
#   'type1' => [
#     [ 'predicate', 'object_datatype' ],
#     [ 'predicate', 'object_datatype' ],
#   ],
#   'type2' => [
#     [ 'predicate', 'object_datatype' ],
#     [ 'predicate', 'object_datatype' ],
#   ]
# }

require 'yaml'
def output_tpts( tpts = {}, path = File.expand_path( './result.tpts.yml' ) )
  File.open( path, 'w' ) do |file|
    file.write( tpts.to_yaml )
  end
end

def subject_reduced_tpts
  tpts = Hash.new { |type, key| type[key] = Array.new }

  Dir.glob( File.join( RDF_PATH, '*.n3' ) ) do |file| # TODO: generalize
    RDF::N3::Reader.open( file ) do |reader|          # TODO: generalize
      current_rdf_type = nil

      reader.each_statement do |stm|
        current_rdf_type = stm.object.to_s if stm.predicate == RDF.type
        datatype = detect_datatype( stm )
        predicate = stm.predicate.to_s

        tpts.store( current_rdf_type, Array.new ) unless tpts.key? current_rdf_type
        predicates = tpts[current_rdf_type].map { |record| record[0] }

        # TODO: frequency counting
        tpts[current_rdf_type] << [ predicate, datatype ] unless predicates.index predicate
      end
    end
  end
  output_tpts( tpts, File.expand_path( './subject_reduced.tpts.yml' ) )
end

def store_as_tpts
  tpts = Hash.new { |type, key| type[key] = Hash.new { |subject, name| subject[name] = Array.new } }

  # iterate XML files
  Dir.glob( File.join( RDF_PATH, '*.n3' ) ) do |file| # TODO: generalize
    # puts file
    RDF::N3::Reader.open( file ) do |reader|          # TODO: generalize
      current_rdf_type = nil

      reader.each_statement do |stm|
        current_rdf_type = stm.object.to_s if stm.predicate == RDF.type
        datatype = detect_datatype( stm )
        subject = stm.subject.to_s
        predicate = stm.predicate.to_s
        object = stm.object.to_s

        tpts.store( current_rdf_type, { subject => Array.new } ) unless tpts.key? current_rdf_type
        tpts[current_rdf_type].store( subject, Array.new ) unless tpts[current_rdf_type].key? subject
        tpts[current_rdf_type][subject] << [ predicate, object, datatype.to_s ]
      end
    end
  end
  output_tpts( tpts )
end

def detect_datatype( stm )
  # object's datatype detection
  if stm.object.class <= RDF::Literal # Literal
    if stm.object.has_datatype?
      datatype = stm.object.datatype.to_s
    else
      # TODO: find datatpe
      datatype = 'RDF::Literal::String'
    end
  elsif stm.object.class == RDF::URI # Resource
    # look up next resource
    datatype = "RDF::URI"
  end
  datatype
end

# store_as_tpts
subject_reduced_tpts

