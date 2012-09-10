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

    def rdf( method, scinets_id )
      result = invoke( [ method, scinets_id ] )
      result = result['list']

      triples = []
      result.each do |set|
        create_rdf( set ).each do |triple|
          triples << triple
        end
      end
      triples
    end

    def recursive_rdf( destination, predicate, option = { } )
      list = recursive_invoke( destination, predicate, option )
      rdf_list = []
      list.each { |x| rdf_list << create_rdf( x ) }
      rdf_list
    end

    def recursive_invoke( destination, predicate, option = { } )
      result = invoke( destination, option )
      if result.class == Array || result.class == Hash
        result = result['list']
      end

      triples = []
      result.each do |set|
        set_predicate = set['property']['ID']
        name = get( 'name', set_predicate, option )
        if name == predicate
          # rdf = create_rdf( set ); triples << set # it doesn't work correctly :(
          triples << set
          triples << recursive_invoke( [ 'statements', set['object']['ID'] ], predicate, option )
        end
      end
      triples.flatten
    end

    def get( method, scinets_id, option = { } )
      result = invoke( [ method, scinets_id ], option )

#      result.key? 'list' ? result['list'].first[method] : result[method] # balky :(

      if result.key? 'list'
        if result['list'].first.key? method
          result['list'].first[method]
        else
          result['list'].first
        end
      else
        result[method]
      end
    end

    #=======================================================
    private
    #=======================================================

    def invoke( destination, option = { } )
      if destination.class == Array
        uri = generate_uri( destination, option )
        result = invoke_by_uri( uri )
#        result['list']
        result
      elsif destination.class == String # URI
        result = invoke_by_uri( destination, option )
#        result['list']
        result
      end
    end

    def create_rdf( set )

      list = []

      subject = set['subject']['ID']
      predicate = set['property']['ID']
      object = set['object']

      if object.class == Array && object.size >= 2
        object.each do |obj|
          list << build_triple( subject, predicate, obj )
        end
#      elsif object.class == Array && object.size == 2
#        pp object
#        pp object[0] === object[1]
#        STDIN.gets
      else
        list << build_triple( subject, predicate, object )
      end

      list
    end

    def build_triple( subject, predicate, object )
      rdf = RDF::Statement.new
      rdf.subject = SCINETS_ITEM + subject
      rdf.predicate = SCINETS_ITEM + predicate

      if object.key? 'value'
        literal = RDF::Literal.new( object['value'], :datatype => object['literalDataType'] || 'xsd:string' )
        rdf.object = literal
      else
        rdf.object = SCINETS_ITEM + object['ID']
      end

      rdf
    end

    def invoke_by_uri( json_uri, option = { } )

      @last_requested_uri = json_uri
      RestClient.proxy = ENV["http_proxy"]
      response = RestClient.get( json_uri )

      case response.code
      when 200
        begin
          json = JSON.parse( response.to_str )
        rescue => exp
          puts exp.message
          puts exp.backtrace
        end
        json
      when 500
        nil
      end
    end

    def get_name( scinets_id )
      result = invoke( [ 'name', scinets_id ] )
      result['name']
    end

    def get_type( scinets_id )
      result = invoke( [ 'type', scinets_id ] )
      result['list'].first['type']
    end

    def get_label( scinets_id )
      result = invoke( [ 'label', scinets_id ] )
      result['label']
    end

    def generate_uri( destination, option = { } )
      command = destination[0]
      scines_uri = destination[1]
      lang = option[:lang] || 'en' # lang = destination[2] || 'en'
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
