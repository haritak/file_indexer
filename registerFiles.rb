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

#DB.loggers << L
#DB.drop_table?(:_duplicates)
#DB.drop_table?(:_files)

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



S = Mutex.new

_Files = DB[:_files]
_Dups = DB[:_duplicates]

baseDir = "."
baseDir = ARGV[0] if ARGV.size >= 1
baseDir = File.join(baseDir, "**/*")

L.info("Searching for files")
Dir.glob(baseDir).each_with_index do |fn,i|
	L.info ("Search finished") if i==1

	while Thread.list.count>10 
		sleep 1 while Thread.list.count > 4
		puts Thread.list.count
	end

	Thread.new do 
		local_fn = ""
		S.synchronize {
			local_fn = fn
		}

		apath = File.realpath(local_fn)
		Thread.exit unless File.exists?(apath)
		Thread.exit unless File.readable?(apath)
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

		retries = 0;
		begin
			if _Files.select.where(:sha=>"#{sha}").first
				_Dups.insert(:sha=>sha, :apath=>apath, 
					:bytes=>size, :seconds=>modTime)
				retries=0;
			else
				begin
					_Files.insert(:sha=>sha, :apath=>apath, 
					:bytes=>size, :seconds=>modTime)
				rescue Sequel::UniqueConstraintViolation => e
					#Give it another try, as another thread
					#may have inserted the same sha
					#after we checked _Files
					retries += 1
					if retries>2 
						raise e
					end
				end
			end

		end while retries==1
	end #thread
end #Dir.glob
puts "Waiting for threads to finish"
sleep 1 while Thread.list.count>1
puts _Files.count


