require 'rubygems'
require 'sequel'

def main
  host = HighLine.new.ask ( 'Hostname: ' )
  user = HighLine.new.ask( 'MySQL User: ' )
  passwd = HighLine.new.ask( 'MySQL Password: ' ) { |q| q.echo = '*'}
  dbname = HighLine.new.ask( 'Database Name: ' )

  db = Sequel.connect( "mysql://#{user}:#{passwd}@#{host}/#{dbname}", { :encoding => 'utf8' } )

  db.create_table :times do
    primary_key :id
    column :year, :int
    column :month, :int
    column :day, :int
    column :hour, :int
    column :minutes, :int
    column :second, :int
    column :milli_second, :int
  end

  db.create_table :places do
    primary_key :id
    String :area
    String :country
    String :state
    String :county
    String :name
    # latitude, longitude, altitude
  end

  db.create_table :sensors do
    primary_key :id
    String :type
  end

  db.create_table :observations do
    primary_key :id
    foreign_key :place_id, :places
    foreign_key :time_id, :times
    foreign_key :sensor_id, :sensors
    # value
  end

end

main
