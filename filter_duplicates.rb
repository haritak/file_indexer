#!/usr/bin/ruby
#require 'rubygems'
require 'mysql'
require 'sequel'
require 'logger'
require 'thread'
require 'digest'
load '/home/yannos/scripts/credentials.txt'

Thread::abort_on_exception=true

DB = Sequel.connect("mysql://#{MY_USERNAME}:#{MY_PASSWORD}@localhost/yannos")
Ldb = Logger.new('filter_duplicates.db.log')
L = Logger.new('filter_duplicates.ok.log')
Lunlinked = Logger.new('filter_duplicates.unlinked.log')
Ler = Logger.new('filter_duplicates.error.log')

DB.logger = Ldb

_Files = DB[:_files]
_Dups = DB[:_duplicates]

def unlinkFiles(myap, oap, mysz)
	@bytes += mysz
	if not File.writable?(myap)
		Ler.warn ("File not writable #{myap}")
		return false
	end

	Lunlinked.info("Unlinking #{myap}")
	File.unlink(myap)
	File.link(oap, myap)
	Lunlinked.info ("Linked to #{oap}")
	return true
end

@counter = 0;
@bytes = 0;
@start = Time.now
semaphore = Mutex.new
_Dups.select(:sha, :apath, :bytes, :seconds).all do |row|


	@counter += 1

	puts
	puts "--- #{@counter} ---"
	puts Time.now - @start
	puts

	sha = ap = sz = mt = ""
	semaphore.synchronize {
		sha = row[:sha]
		ap =  row[:apath]
		sz = row[:bytes]
		mt = row[:seconds]
	}
	
	mysha = myap = mysz = mymt = " "
	semaphore.synchronize {
		mysha = sha
		myap = ap
		mysz = sz
		mymt = mt
	}

	#get the original file
	o = _Files.select(:sha, :apath, :bytes, :seconds).first(:sha=>mysha)
	if not o 
		Lerr.warn("sha not found for #{myap}")
		next
	end
	
	oap = o[:apath]

	L.info("Working on #{myap} which is similar in content to #{oap}")

	if oap == myap
		L.info("They are the same")
		next
	end

	if not File.exists?(oap)
		L.info("Original file has been deleted")
		next
	end

	if not File.exists?(myap)
		L.info("Duplicate file has been deleted")
		next
	end


	if File.identical?(oap, myap)
		L.warn("Files are identical")
		next
	end

	osz = File.size(oap)
	omt = File.mtime(oap).to_f
	

	if osz == mysz && omt == mymt &&
		File.size(myap) == mysz && File.mtime(myap).to_f == mymt	
		
		if not unlinkFiles(myap, oap, mysz) 
			next
		end
	else
		puts "Recalculating hashes"
		myNewSha = ''
		oNewSha = ''
		t1 = Thread.new do
			myNewSha = Digest::SHA256.file(myap)
		end
		t2 = Thread.new do 
			oNewSha = Digest::SHA256.file(oap)
		end

		puts "Waiting for thread 1 to finish calculating its hash"
		t1.join
		puts "Waiting for thread 2 to finish calculating its hash"
		t2.join

		if myNewSha && oNewSha && myNewSha==oNewSha
			unlinkFiles(myap, oap, mysz)
		else
			Ler.warn ("Files differ:")
			Ler.warn ("My file #{myap}")
			Ler.warn ("My sha #{myNewSha}")
			Ler.warn ("Original #{oap}")
			Ler.warn ("Original sha #{oNewSha}")
			Ler.warn ("Original file attributes are #{osz}, #{omt}")
			Ler.warn ("My file attributes are #{mysz}, #{mymt}")
		end
	end

end #iteration over all rows

unit = ['b', 'K', 'M', 'G'].reverse
4.times do
	print @bytes
	puts unit.pop
	@bytes = @bytes/1000
end

