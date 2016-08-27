#!/usr/bin/ruby
require 'mysql'
require 'sequel'
require 'optparse'

load 'credentials.txt'

options = {}
options_parser = OptionParser.new do |opts|
	opts.on("-D", "--duplicates") do
		options[:duplicates] = true
	end
	opts.on("-l", "--list") do
		options[:list] = true
	end
	opts.on("-s", "--size") do
		options[:size] = true
	end
end

options_parser.parse!

DB = Sequel.connect("mysql://#{MY_USERNAME}:#{MY_PASSWORD}@localhost/#{DB_NAME}")

_Table = DB[:_files]
_Table = DB[:_duplicates] if options[:duplicates]

puts _Table.count

if options[:size] 
	total_bytes =  _Table.sum(:bytes)
	kBytes = total_bytes/1024
	mBytes = total_bytes/1024/1024
	gBytes = total_bytes/1024/1024/1024
	puts "%d GiBytes or %d MiBytes (%d bytes)" % [gBytes, mBytes, total_bytes]
end
if options[:list] 
	_Table.each do |l|
		puts l
	end
end

