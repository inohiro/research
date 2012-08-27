require './lib/json_client.rb'
require 'pp'

require 'rdf'
require 'rdf/json'
require 'json'


def main( argv )

  client = SemanticJson::JsonClient.new( "http://semantic-json.org" )
  
#  result = client.invoke( ['statements', 'AT5G62480.1' ] )
#  result = client.invoke( ['statements', 'cria93s1ria93s1i' ] )
#  result = client.invoke( [ 'statements', 'crib166u26rib166u3702i' ] )

  result = client.invoke( %w( statements crib166u26rib166u3702i) )

  pp result['list']
  list = result['list']

  rdf = RDF::JSON::Reader.new( JSON.generate( list ) )


  # AT5G62480.1

  # json = client.invoke( "http://semantic-json.org/json/name/ja/rib158i")
  # hoge = client.invoke( [ "name", "rib158i" ] )

  # pp json
  # pp hoge

end

main( ARGV )
