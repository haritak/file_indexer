#!/usr/bin/ruby

require 'mysql'
require 'time'
require 'shellwords'
require 'thread'
load '/home/yannos/scripts/credentials.txt'

class Duplicates
	include Enumerable

	def initialize
		@con = Mysql.new 'localhost', MY_USERNAME, MY_PASSWORD, 'yannos'
		@rs = @con.query("SELECT sha256,fullpath FROM duplicate_files")
		puts @rs.num_rows
	end

	def each
		while row = @rs.fetch_row do 
			yield row
		end
	end

	def close
		@con.close if @con
	end


	def find_original(sha)
                rs = @con.query ("SELECT fullpath FROM files WHERE sha256=\"#{sha}\";")
                raise "Unpredictable error" if rs.num_rows > 1
		rs.num_rows == 1 ? rs.fetch_row[0] : nil;
        end
end

def calcSha(ap)
	cmd = "sha256sum \"#{ap}\""
	s = `#{cmd}`
	s.length<64 ? nil : s[0..63]
end

class Logger
        def initialize(filename)
                @log = File.new(filename, "a")
                @log.puts("--- new run ---")
        end

        def log(f)
                @log.puts(f)
        end

        def close
                @log.close if @log
        end
end

begin

	dups = Duplicates.new
	log = Logger.new("filter_duplicates.log")
	failed = Logger.new("filter_duplicates.failed.log")

	@counter = 0;

	@start = Time.now

	semaphore = Mutex.new

	dups.each do |row|

		sleep 10 while Thread.list.count > 50

		Thread.new do
		begin
			sha = ap = o_ap = ''
			semaphore.synchronize {
				sha = row[0]
				ap =  row[1]
				o_ap = dups.find_original(sha)
			}

			if ap==o_ap
				print "="
				Thread.exit
			elseif File.identical?(ap, o_ap) 
				print "."
				Thread.exit
			else 

				#new_sha = calcSha( ap )
				if (new_sha!= sha )
					 raise "Shas missmatch!"
				end
				#new_sha = calcSha( o_ap )
				if (new_sha!= sha )
					 raise "Shas missmatch 2!"
				end
				if (File.size(ap) != File.size(o_ap)) 
					raise "Files with the same sha, but different size!"
				end
				
				if File.writable?(ap)
					semaphore.synchronize {
						log.log("Unlinking #{ap}, Linking with #{o_ap}")
						@counter+=1
					}
					#File.unlink(ap)
					#File.link(o_ap, ap)
					print "+"
				else
					puts ap
					exit
					print "!"
					semaphore.synchronize {
						failed.log("Not writable : #{ap}")
					}
				end
			end
		end

		end #thread
	end

rescue Mysql::Error => e
    puts e.errno
    puts e.error

ensure
	dups.close if dups
	log.close if log
	failed.close if failed

	elapsed = Time.now - @start
	
	puts "#{@counter} files were linked"
	puts "#{elapsed} seconds"
end
