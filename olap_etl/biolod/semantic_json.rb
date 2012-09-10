require './lib/json_client.rb'
require 'pp'

require 'rdf'
require 'rdf/json'
require 'json'


def main( argv )

  method = argv[0] || 'statements'
  scinets_id = argv[1] || 'crib166u26rib166u3702i'

  client = SemanticJson::JsonClient.new( "http://semantic-json.org" )

#  result = client.invoke( ['statements', 'AT5G62480.1' ] )
#  result = client.invoke( ['statements', 'cria93s1ria93s1i' ] )
#  result = client.invoke( [ 'statements', 'crib166u26rib166u3702i' ] )

#  result = client.invoke( %w( statements crib166u26rib166u3702i) )
#  result = client.invoke( [ method, scinets_id ])
#  result = client.get( 'statements', scinets_id )
  result = client.get( method, scinets_id )
  puts client.last_requested_uri

  pp result

  label = client.get( 'label', scinets_id )
  puts "label: #{label.to_s}"
  type = client.get( 'type', scinets_id )
  puts "type: #{type.to_s}"
  name = client.get( 'name', scinets_id, { :lang => 'ja' } )
  puts "name: #{name.to_s}"

#  if result.class == Array || result.class == Hash
#    result = result['list']
#  end

#  rdf = RDF::JSON::Reader.new( JSON.generate( list ) )

=begin
  if result
    result.each do |stm|
      pp stm
      p '==========================='
    end
  end
=end


#  triples = client.rdf( method, scinets_id )
#  pp triples

#  puts "requested: #{client.last_requested_uri}"

#  triples.each do |triple|
#    pp triple
#  end

  rec_triples = client.recursive_invoke( [ method, scinets_id ], 'parent node', { :lang => 'ja' } )
#  rec_triples = client.recursive_invoke( [ method, scinets_id ], 'belong_to' )

#  pp rec_triples

  rec_triples.each do |triple|
#    pp triple
#    puts client.get_name( triple['subject']['ID'] )
    puts client.get( 'name', triple['subject']['ID'], { :lang => 'ja' } )
  end

#  rec_rdf = client.recursive_rdf( [method, scinets_id], 'parent node' )
#  pp rec_rdf

  # AT5G62480.1

  # json = client.invoke( "http://semantic-json.org/json/name/ja/rib158i")
  # hoge = client.invoke( [ "name", "rib158i" ] )

  # pp json
  # pp hoge

end

main( ARGV )
