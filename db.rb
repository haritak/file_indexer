class Recorder
	def initialize
		@con = Mysql.new 'localhost', MY_USERNAME, MY_PASSWORD, 'yannos'
		rs = @con.query("SELECT * FROM files where sha256='1'") #testing connection
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
		cmd = "UPDATE files SET \
                             sha256=\"#{sha}\", \
                             size=#{sz}, \
                             moddate=#{mt} \
                             WHERE binary fullpath=\"#{ap}\";"
		#puts cmd
		@con.query (cmd)
	end


	def checkFilenames
	#TODO : put a comment why is this here

		rs = @con.query("SELECT * FROM files")	

		puts rs.num_rows 

		while row = rs.fetch_row do
			fn = row[1]
			next unless File.exists?(fn)
			sha = row[0]
			if fn =~ /[!@#$%^&*={};'\\:"|,<>]/
				newSha = calcSha(fn)
				if newSha != sha 
					puts "Found mistake for #{fn}"
					updateFile(fn, newSha, File.size(fn), File.mtime(fn).to_f)
					puts "Mistake corrected"
				end
			end
		end
	end


private

	def image?(fn)
	   ext = File.extname(fn).downcase;
	   ext==".jpg" or ext == ".jpeg" or ext == ".gif" or ext == ".cr2"
	end

end

