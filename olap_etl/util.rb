require 'rubygems'
require 'sequel'

PASSWORD = ''

class Util

  #
  #== Util.connect_db
  #
  # return Sequel DB object
  #
  def self.connect_db( p )

    user =     p[:user]     || 'root'
    passwd =   p[:password] || PASSWORD
    host =     p[:host]     || 'localhost'
    db =       p[:db]       || 'test2'
    db_type =  p[:db_type]  || 'mysql'
    encoding = p[:encoding] || 'utf8'

    Sequel.connect( "#{db_type}://#{user}:#{passwd}@#{host}/#{db}",
                    { :encoding => encoding} )
  end

  #
  #== Util.connect_gn
  #
  # return Sequel DB Object that connects GeoNames Database
  #
  def self.connect_gn( p )

    db = p[:db] || 'geonames_rdf'

    self.connect_db( { :user =>     p[:user],
                       :password => p[:password],
                       :host =>     p[:host],
                       :db =>       db,
                       :db_type =>  p[:db_type],
                       :encoding => p[:encoding] } )
  end

  #
  #== Util.geonames_id
  #
  # return geonames id from geonames uri
  # params: http://sws.geonames.org/7452809/
  # return: 7452809
  #
  def self.geonames_id( geonames_uri )
    /\//.match( /org\//.match( geonames_uri ).post_match ).pre_match
  end

  #
  #=== Util.chop_uri
  #
  # return choped uri
  # params: http://www.w3.org/2001/XMLSchema#string
  # result: string
  #
  def self.chop_uri( uri )
    if m = /\#/.match( uri.to_s )
      m.post_match
    else
      nil
    end
  end

  #
  #== Util.valid_http_uri?
  #
  # if argument uri has started 'http', return true
  #
  def self.valid_http_uri?( uri )
    URI.parse( uri ).scheme == 'http' rescue false
  end
end
