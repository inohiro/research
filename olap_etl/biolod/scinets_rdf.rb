require 'rdf'
require 'hpricot'
require 'rest-client'
require 'pp'
require './../util.rb'

Uri = ['https://bladegw2.base.riken.jp/sw/en/Shewanella/crib166u22rib166u22i/']

def print_help
  puts <<EOS
usage: ruby scinets_rdf.rb URI
EOS
end

def html_to_rdf( html, option = { :type => :ntriples } )
  instance_name = html.search( "div.title" ).search( "a" ).inner_text
  pp instance_name

  trs = html.search( "tr.crawlBgPlain" )
  pp trs
  
end

def get_html( uri )
  response = RestClient.get( uri )
  return nil if response.code != 200 # hmm
  Hpricot( response.to_str )
end


def main( argv )
  if argv.length != 1
    print_help
    return
  end

  uri = argv[0]
#  return unless Util.valid_http_uri?( uri )

  html = get_html( uri )

  if html == nil
    puts 'HTTP GET failed...'
    return
  else
    rdf = html_to_rdf( html, { :type => :xml } )
  end

end

# main( ARGV )
main( Uri )
