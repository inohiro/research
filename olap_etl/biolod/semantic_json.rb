require './lib/json_client.rb'
require 'pp'

require 'rdf'
require 'rdf/json'
require 'json'


def main( argv )

  method = argv[0]
  scinets_id = argv[1]

  client = SemanticJson::JsonClient.new( "http://semantic-json.org" )
  
#  result = client.invoke( ['statements', 'AT5G62480.1' ] )
#  result = client.invoke( ['statements', 'cria93s1ria93s1i' ] )
#  result = client.invoke( [ 'statements', 'crib166u26rib166u3702i' ] )

#  result = client.invoke( %w( statements crib166u26rib166u3702i) )
  result = client.invoke( [ method, scinets_id ])


  if result.class == Array || result.class == Hash
    result = result['list']
  end

#  rdf = RDF::JSON::Reader.new( JSON.generate( list ) )

  result.each do |stm|
    pp stm
    p '==========================='
  end


  triples = client.rdf( [ method, scinets_id ] )
  
  puts "requested: #{client.last_requested_uri}"

  triples.each do |triple|
    pp triple
  end

  rec_triples = client.recursive_invoke( [ method, scinets_id ], 'parent node' )
#  pp rec_triples

  rec_triples.each do |triple|
    puts client.get_name( triple['subject']['ID'] )
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
