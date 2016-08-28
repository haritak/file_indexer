#!/usr/bin/ruby
#require 'rubygems'
require 'mysql'
require 'sequel'
require 'logger'
require 'thread'
require 'digest'
load 'credentials.txt'

Thread::abort_on_exception=true
MAX_THREADS = 10

DB = Sequel.connect("mysql://#{MY_USERNAME}:#{MY_PASSWORD}@localhost/#{DB_NAME}")
L = Logger.new($stdout)
notFound_L = Logger.new('Files_Not_Registered_In_Database.txt')
warnings = Logger.new('Warnings.txt')

S = Mutex.new
LS = Mutex.new #for logging

_Files = DB[:_files]
_Dups = DB[:_duplicates]

baseDir = "."
baseDir = ARGV[0] if ARGV.size >= 1
baseDir = File.join(baseDir, "**/*")

puts "There are currently #{_Files.count} files recorded as single"

notRegisteredFiles = 0;

L.info("Searching for not registered files.")
Dir.glob(baseDir).each_with_index do |fn,i|
	L.info ("Search finished") if i==1

        sd=0.01
	while Thread.list.count>10 
		while Thread.list.count > MAX_THREADS
			sleep sd 
			sd = 2*sd
			puts Thread.list.count
		end
	end

	Thread.new do 
		local_fn = ""
		S.synchronize {
			local_fn = fn
		}

		Thread.exit unless File.exists?(local_fn)

		apath = File.realpath(local_fn)
		if not File.readable?(apath)
			LS.synchronize {
				warnings.warn("not readable: #{apath}") 
			}
			Thread.exit 
		end
		Thread.exit unless not File.directory?(apath)

		size = File.size(apath)
		modTime = File.mtime(apath).to_f

		same_apath = _Files.select(:sha, :bytes, :seconds)
		.where(:apath=>"#{apath}").first

		Thread.exit if (same_apath && 
				same_apath[:bytes]==size &&
				same_apath[:seconds]==modTime)

		same_apath_dup = _Dups.select(:sha, :bytes, :seconds)
		.where(:apath=>"#{apath}").first

		Thread.exit if (same_apath_dup && 
				same_apath_dup[:bytes]==size &&
				same_apath_dup[:seconds]==modTime)

		s = Digest::SHA256.file(apath)
		sha = "#{s}"

		if (same_apath && same_apath[:sha]!=sha)
			same_apath.update(:sha=>sha, 
			:bytes=>size, :seconds=>modTime)
			Thread.exit
		end

		if not _Files.select.where(:sha=>"#{sha}").first
			begin
				LS.synchronize {
					notFound_L.warn("Not registered:#{apath}")
					notRegisteredFiles += 1
				}
			end
		end
	end #thread
end #Dir.glob
puts "Waiting for threads to finish"
sleep 1 while Thread.list.count>1

puts "#{notRegisteredFiles} were not registered"


