require 'rdf'
require 'rdf/n3'
require 'rdf/rdfxml'

require 'rubygems'
require 'pp'
require 'highline'
require 'sequel'


# OBSERVATORY_PATH = "file:/Users/inohiro/Projects/rdf_rb/WSFO3_2005_8_26.n3"
OBSERVATORY_PATH = "file:/Users/inohiro/Projects/rdf_rb/SNHUT_2004_8_11.n3"

def main
  observatory = RDF::Graph.load( OBSERVATORY_PATH )
  observatory.each_statement do |stm|
#    pp stm.inspect
    pp stm.subject
    pp stm.predicate
    pp stm.object
    if stm.object.class == RDF::Literal
      if stm.object.has_datatype?
        p '##############################'
        puts "Value: #{stm.object.to_s}, DataType: #{stm.object.datatype.to_s}"
      end
    elsif stm.object.class == RDF::URI
      p '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'
      pp stm.object
      unless stm.object =~ /ssw/
        graph = RDF::Graph.load( stm.object )
        graph.each_statement do |g|
          if g.predicate == RDF.type
            p '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
            pp g.object
          end
        end
      end
    end
    p '=============================='
  end

end

main
