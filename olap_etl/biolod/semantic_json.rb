require './lib/json_client.rb'
require 'pp'

client = SemanticJson::JsonClient.new( "http://semantic-json.org" )

json = client.invoke_by_uri( "http://semantic-json.org/json/name/ja/rib158i")
hoge = client.invoke( [ "name", "rib158i" ] )

pp json
pp hoge

