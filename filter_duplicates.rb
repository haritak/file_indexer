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
L = Logger.new($stdout)
DB.logger = L

_Files = DB[:_files]
_Dups = DB[:_duplicates]

@counter = 0;
@start = Time.now
semaphore = Mutex.new
_Dups.select(:sha, :apath, :bytes, :seconds).all do |row|

	sha = ap = sz = mt = ""
	semaphore.synchronize {
		sha = row[:sha]
		ap =  row[:apath]
		sz = row[:bytes]
		mt = row[:seconds]
	}
	
	Thread.new do

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
			L.warn("sha not found")
			Thread.exit
		end
		
		oap = o[:apath]

		L.info("Working on #{myap} which is similar in content to #{oap}")

		if oap == myap
			L.info("They are the same")
			Thread.exit
		end

		if not File.exists?(oap)
			L.info("Original file has been deleted")
			Thread.exit
		end

		if not File.exists?(myap)
			L.info("Duplicate file has been deleted")
			Thread.exit
		end


		if File.identical?(oap, myap)
			L.warn("Files are identical")
			Thread.exit
		end

		osz = File.size(oap)
		omt = File.modtime(oap)
		

		if File.size(oap) == mysz &&
			File.modtime(oap) == mymt &&
			File.size(myap) == mysz &&
			File.modtime(myap) == mymt	

			L.info("Unlinking #{myap}"
			File.unlink(myap)
			File.link(oap, myap)
			L.info ("Linked to #{oap}")
		end


	

	end #thread


	sleep 1 while Thread.list.count>1 

	exit

end #iteration over all rows


