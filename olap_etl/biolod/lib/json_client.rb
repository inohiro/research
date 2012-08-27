# -*- coding: utf-8 -*-

require 'rest-client'
require 'json'

module SemanticJson
  class JsonClient
    attr_reader :server_url
    attr_reader :last_requested_uri
  
    def initialize( server_url )
      @server_url = server_url << '/json'
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

    #=======================================================
    private
    #=======================================================

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
