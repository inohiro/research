require 'rest_client'

require 'pp'

OUTPUT_DIR = './outputs/'
BASE_URL = 'http://www.kanzaki.com/works/2011/stat/ra/'
# http://www.kanzaki.com/works/2011/stat/ra/20110328/

#
#== main
#
# crawl observed radiational data
# (1st March 2011 -> 31st December 2011)
#
def main

  unless File.exists?( OUTPUT_DIR )
    Dir::mkdir( OUTPUT_DIR )
  end

  ( 3..12 ).each do |m| # month
    span = ( 1..31 )
    case m when 4, 6, 9, 11 then span = ( 1..30 ) end

    span.each do |d| # date
      date = '2011'
      if m < 10
        date = date + '0' + m.to_s
      else
        date = date + m.to_s
      end
      if d < 10
        date = date + '0' + d.to_s
      else
        date = date + d.to_s
      end

      uri = BASE_URL + date
      begin
        result = RestClient.get( uri, :timeout => 10 )
        file_path = OUTPUT_DIR + date + '.rdf'
        pp file_path
        open( file_path, 'w' ) do |f|
          f.write( result )
        end
      rescue => exp
        pp exp
      end
      sleep( Random.new.rand( 3..20 ) )
    end
  end
end

main
