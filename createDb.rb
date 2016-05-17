#!/usr/bin/ruby
require 'mysql'
require 'sequel'
require 'logger'
load '/home/yannos/scripts/credentials.txt'

puts "Database will be wiped out in 5 seconds"

sleep 6

DB = Sequel.connect("mysql://#{MY_USERNAME}:#{MY_PASSWORD}@localhost/yannos")
L = Logger.new($stdout)

DB.loggers << L
DB.drop_table?(:_duplicates)
DB.drop_table?(:_files)

DB.create_table?(:_files) do
	String :sha, :fixed=>true, :size=>64, :index=>true, :primary_key=>true
	String :apath, :text=>true, :size=>5000
	Bignum :bytes
	Float :seconds
end

DB.create_table?(:_duplicates) do
	String :sha, :fixed=>true, :size=>64, :index=>true
	String :apath, :text=>5000, :fixed=>true
	Bignum :bytes
	Bignum :seconds
	foreign_key [:sha], :_files
end

