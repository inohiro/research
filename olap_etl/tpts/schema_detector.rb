# coding: utf-8

BASE_DIR = File.expand_path './'

def main

  # schema = {
  #   'SalesResult' => [],
  #   'Time' => [
  #     {  :name=>"subject", 
  #        :resource_type=>"resource", 
  #        :data_type=>"URI"
  #     },
  #     {  :name=>"year",
  #        :resource_type=>"literal",
  #        :data_type=>"integer"
  #     },
  #     {  :name=>"month",
  #        :resource_type=>"literal",
  #        :data_type=>"integer"
  #     }
  #   ]
  # }

  Dir.glob( BASE_DIR + '*.xml' ) do |file|
    path_to_file = 'file:' + f.to_s

    schema = Hash.new { |h,k| h[k] = Array.new }

    statements = RDF::RDFXML::Reader.open( path_to_file )
    statements.each do |statement|

      if statement.predicate == RDF.type # 'rdf:type'
        # found a new 'rdf:type', add the type to schema entry
        schema.store( statement.object.to_s, Array.new )

      else
        # resource_type = RDF::Resource || RDF::Literal
        resource_type = resource_type_detector( stm.object.class )
        data_type = statement.object.class.to_s if statement.object.has_datatype?

        entry = {
          name: statement.predicate.to_s,
          resource_type: resource_type,
          data_type: data_type,
        }

        current_rdf_type = schema[statement.object.to_s]
      end
      
    end
  end

end

def resource_type_detector( object_class )
  if object_class <= RDF::Literal
    'Literal'
  elsif object_class == RDF::URI
    'URI'
  else
    'unknown'
  end
end

main
