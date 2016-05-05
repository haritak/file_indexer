#!/usr/bin/ruby

require 'mysql'
require 'time'
require 'digest'
load '/home/yannos/scripts/credentials.txt'
load '/home/yannos/scripts/file_indexer/logger.rb'
load '/home/yannos/scripts/file_indexer/db.rb'

def calcSha(ap)
	s = Digest::SHA256.file ap
end


begin
	
	puts "Initializing"
	log = Logger.new
	recorder = Recorder.new
	puts "Initialized"

	@start = Time.now
	@counter = 0


	Dir.glob("**/*").each_with_index do |fn,i|
	  @counter+=1
	  if @counter== 1 
		puts "Started"
	  end

	  while Thread.list.count > 50 do
		puts "waiting for some threads to finish"
		while Thread.list.count > 25 do
			puts Thread.list.count
			sleep 1
			puts Thread.list.count
			sleep 1
		end
	  end
	  Thread.new do
 		  local_fn = fn
		  if not File.directory?(local_fn)

			begin
				ap = File.realpath(local_fn)
			rescue => e
				log.log_skipped(local_fn, "File.realpath fails, probably broken link")
				Thread.exit
			end
			if not File.readable?(ap) 
				log.log_skipped(ap, "not readable")
				Thread.exit
			end

			puts "----Processing file number #{i} ----"
			p ap
			puts ap

			sz = File.size(ap);
			mt = File.mtime(ap).to_f;

			puts "#{sz}", "#{mt}"

			if recorder.exists_file?(ap)  
				p_sz = recorder.getSize(ap).to_i
				p_mt = recorder.getModTime(ap).to_f
				
				puts "File was found with size #{p_sz} and modification time #{p_mt}"
				
				if p_sz == sz && p_mt == mt
					log.log_skipped(ap, "same size, same mt already recorded")
					puts "same size, same mod time, skipped"
				else
					sha = calcSha(ap)
					puts "Record updated due to file contents change"
					recorder.updateFile(ap, sha, sz, mt)
				end
			else
				sha=calcSha(ap) 

				if recorder.exists_sha?(sha) 
					previous_ap = recorder.getFullpath(sha);

					if File.identical?(ap, previous_ap)
						log.log_skipped(ap, "Identical as #{previous_ap}")
						puts "Is identical to #{previous_ap}"
				
					else
						puts "Recorded as duplicate to #{previous_ap}"
						recorder.insertAsDuplicate(sha, ap)
					end
				else
					puts "Recorded as brand new"
					recorder.insertToDb(sha, ap, sz, mt) 
				end
			end
		end

		puts ""
		Thread.exit
	  end

	end

	puts("--- done ! ---")

rescue Mysql::Error => e
    puts e.errno
    puts e.error

ensure
	recorder.close if recorder
	log.close if log

	elapsed = Time.now - @start
	puts "Elapsed time #{elapsed} seconds #{elapsed} #{elapsed}"
end
