#!/usr/bin/ruby

require 'mysql'
require 'time'
load '/home/yannos/scripts/credentials.txt'

class Logger
	def initialize
		@log = File.new("/home/yannos/dirlisting.errors.log", "a")
		@log.puts("--- new run ---")
	end

	def log_skipped(f, r)
		@log.puts("")
		@log.puts("The following file was skipped")
		@log.puts(f)
		@log.puts(r)
		@log.puts("")
	end
	
	def close
		@log.close if @log
	end
end

class Recorder
	def initialize
		@con = Mysql.new 'localhost', MY_USERNAME, MY_PASSWORD, 'yannos'
		rs = @con.query("SELECT * FROM files") #testing connection
	end

	def close
		@con.close if @con
	end

	def insertToDb(sha, fn, sz, md)
		isImg = image?(fn)
		@con.query ("INSERT INTO \
			files(sha256,fullpath,size,image,moddate) VALUES \
			 (\"#{sha}\", \"#{fn}\", #{sz}, #{isImg}, #{md});")
	end
	def insertAsDuplicate(sha, fn)
		@con.query ("INSERT INTO \
			duplicate_files(sha256,fullpath) VALUES \
			 (\"#{sha}\", \"#{fn}\");")
	end

	def exists_sha?(sha)
		rs = @con.query ("SELECT * FROM files WHERE sha256=\"#{sha}\";")
		rs.num_rows >= 1
	end

	def exists_file?(an)
		#binary makes sure the comparison is case sensitive. 
		#By default comparison is case insesitive. 
		#Moreover, I couldn't find utf8 collation with case sensitivity
		rs = @con.query ("SELECT * FROM files WHERE binary fullpath=\"#{an}\";")
		rs.num_rows >= 1
	end

	def getFullpath(sha)
		rs = @con.query ("SELECT fullpath FROM files WHERE sha256=\"#{sha}\";")
		rs.fetch_row[0]
	end

	def getSize(an)
		rs = @con.query ("SELECT size FROM files WHERE binary fullpath=\"#{an}\";")
		rs.fetch_row[0]
	end

	def getModTime(an)
		rs = @con.query ("SELECT moddate FROM files WHERE binary fullpath=\"#{an}\";")
		rs.fetch_row[0]
	end

	def updateFile(ap, sha, sz, mt)
		@con.query ("UPDATE files SET \
                             sha256=\"#{sha}\", \
                             size=#{sz}, \
                             moddate=#{mt} \
                             WHERE binary fullpath=\"#{ap}\";");
	end


private

	def image?(fn)
	   ext = File.extname(fn).downcase;
	   ext==".jpg" or ext == ".jpeg" or ext == ".gif" or ext == ".cr2"
	end

end



def calcSha(ap)
	cmd = "sha256sum \"#{ap}\""
	s = `#{cmd}`
	s.length<64 ? nil : s[0..63]
end


begin
	
	log = Logger.new
	recorder = Recorder.new

	Dir.glob("**/*").each_with_index do |fn,i|
	  if not File.directory?(fn)

		begin
			ap = File.realpath(fn)
		rescue => e
			log.log_skipped(fn, "File.realpath fails, probably broken link")
			next
		end
		if not File.readable?(ap) 
			log.log_skipped(ap, "not readable")
			next
		end

	        puts "----Processing file number #{i}----"

		sz = File.size(ap);
		mt = File.mtime(ap).to_f;

	        puts "#{sz}", "#{mt}", "#{ap}"

		if recorder.exists_file?(ap)  
			p_sz = recorder.getSize(ap).to_i
			p_mt = recorder.getModTime(ap).to_f
			
			puts "File was found with size #{p_sz} and modification time #{p_mt}"
			
			if p_sz == sz && p_mt == mt
				log.log_skipped(ap, "same size, same mt already recorded")
				puts "same size, same mod time, skipped"
			else
				sha = calcSha(ap)
				if not sha
					log.log_skipped(ap, "bad checksum")
					puts "bad ckecksum #{sha}"
				else
					puts "Record updated due to file contents change"
					recorder.updateFile(ap, sha, sz, mt)
				end
			end
		else
			sha=calcSha(ap) 
			if not sha
				log.log_skipped(ap, "bad checksum")
				puts "bad ckecksum #{sha}"
				next
			end

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

		puts ""

	  end

	end

	puts("--- done ! ---")

rescue Mysql::Error => e
    puts e.errno
    puts e.error

ensure
	recorder.close if recorder
	log.close if log
end
