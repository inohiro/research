# -*- coding: utf-8 -*-

require 'rest-client'
require 'json'
require 'rdf'

SCINETS_ITEM = 'http://scinets.org/item/'

module SemanticJson
  class JsonClient
    attr_reader :server_url
    attr_reader :last_requested_uri
  
    def initialize( server_url )
      @server_url = server_url << '/json'
      @depth = 0
    end

    def invoke( destination )
      if destination.class == Array
        uri = generate_uri( destination )
        result = invoke_by_uri( uri )
#        result['list']
        result
      elsif destination.class == String
        result = invoke_by_uri( destination )
#        result['list']
        result
      end
    end

    def rdf( destination )
      result = invoke( destination )
      result = result['list']

      triples = []
      result.each do |set|
        triples << create_rdf( set )
      end
      triples
    end

    def recursive_rdf( destination, predicate )
      list = recursive_invoke( destination, predicate )
      rdf_list = []
      list.each { |x| rdf_list << create_rdf( x ) }
      rdf_list
    end

    def recursive_invoke( destination, predicate )
      result = invoke( destination )
      if result.class == Array || result.class == Hash
        result = result['list']
      end

      triples = []
      result.each do |set|
        set_predicate = set['property']['ID']
        name = get_name( set_predicate )
        if name == predicate
          # rdf = create_rdf( set ); triples << set # it doesn't work correctly :(
          triples << set
          triples << recursive_invoke( [ 'statements', set['object']['ID'] ], predicate )
        end
      end
      triples.flatten.reverse
    end

    def get_name( scines_id )
      result = invoke( [ 'name', scines_id ] )
      result['name']
    end

    def get_data_type( scines_id )
      result = invoke( [ 'type', scines_id ] )
      result['list'].first['type']
    end

    #=======================================================
    private
    #=======================================================

    def create_rdf( set )
      rdf = RDF::Statement.new
      rdf.subject = SCINETS_ITEM + set['subject']['ID']
      rdf.predicate = SCINETS_ITEM + set['property']['ID']
      rdf.object = SCINETS_ITEM + set['object']['ID']

      object_data_type = set['object']['dataType']

      if object_data_type != 'Instance'
        puts object_data_type
        data_type = get_data_type( set['object']['ID'] )
        name = get_name( data_type )
        puts name
      end
      
      rdf
    end

    def invoke_by_uri( json_uri )

      @last_requested_uri = json_uri
      RestClient.proxy = ENV["http_proxy"]
      response = RestClient.get( json_uri )

      if response.code == 200
        begin
          json = JSON.parse( response.to_str )
        rescue => exp
          puts exp.message
          puts exp.backtrace
        end
      end

      json
    end

    def generate_uri( destination )
      command = destination[0]
      scines_uri = destination[1]
      lang = destination[2] || 'en'
      option = destination[3] || nil

      uri = URI.parse( scines_uri )
      
      if uri.scheme == nil
        scines_id = scines_uri
      else
        path = uri.path
        scines_id = path[path.rindex( '/' )+1..path.size]
      end

      uri = "#{server_url}/#{command}"
      if lang == nil || lang.empty?
        uri = "#{uri}/#{scines_id}"
      else
        uri = "#{uri}/#{lang}/#{scines_id}"
      end

      option == nil || option.empy? ? uri : "#{uri}?#{option}"
    end

    def get_json_object_list( json )
      list = json["list"]
      list.class == Array ? list : [list]
    end
  end
end
