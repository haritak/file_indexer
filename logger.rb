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


