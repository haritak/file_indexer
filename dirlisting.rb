#!/usr/bin/ruby

require 'mysql'
require 'time'
require 'digest'
require 'thread'
load '/home/yannos/scripts/credentials.txt'
load '/home/yannos/scripts/file_indexer/logger.rb'
load '/home/yannos/scripts/file_indexer/db.rb'

def calcSha(ap)
	s = Digest::SHA256.file ap
end

$MAX_NO_THREADS = 50
$MIN_NO_THREADS = 25

semaphore = Mutex.new

begin
	  puts "Initializing"
	  log = Logger.new
	  recorder = Recorder.new
	  puts "Initialized"

	@start = Time.now
	@counter = 0

	puts "Getting directory listing..."

	Dir.glob("**/*").each_with_index do |fn,i|
	  @counter+=1
	  if @counter== 1 
		puts "Started"
	  end

	  while Thread.list.count > $MAX_NO_THREADS do
		puts "waiting for some threads to finish"
		while Thread.list.count > $MIN_NO_THREADS do
			puts Thread.list.count
			sleep 1
			puts Thread.list.count
			sleep 1
		end
	  end

	  Thread.new do
		  semaphore.synchronize {
			  local_fn = fn
		  }
		  if not File.directory?(local_fn)

			begin
				ap = File.realpath(local_fn)
			rescue => e
				semaphore.synchronize {
					log.log_skipped(local_fn, "File.realpath fails, probably broken link")
				}
				Thread.exit
			end
			if not File.readable?(ap) 
				semaphore.synchronize {
					log.log_skipped(ap, "not readable")
				}
				Thread.exit
			end

			myThreadId = Thread.current.object_id
			semaphore.synchronize {
				puts "----Thread number #{myThreadId} is processing file number #{i} ----"
				p ap
				puts ap
			}

			sz = File.size(ap);
			mt = File.mtime(ap).to_f;

			puts "#{sz}", "#{mt}"

			file_exists = false;
			semaphore.synchronize {
				file_exists = recorder.exists_file?(ap)
			}
			if file_exists
				p_sz = 0
				p_mt = 0
				semaphore.synchronize {
					p_sz = recorder.getSize(ap).to_i
					p_mt = recorder.getModTime(ap).to_f
					puts "#{myThreadId}: File was found with size #{p_sz} and modification time #{p_mt}"
				}
				
				
				if p_sz == sz && p_mt == mt
					semaphore.synchronize{
						log.log_skipped(ap, "same size, same mt already recorded")
						puts "#{myThreadId}: same size, same mod time, skipped"
					}
				else
					sha = calcSha(ap)
					semaphore.synchronize {
						puts "#{myThreadId}: Record updated due to file contents change"
						recorder.updateFile(ap, sha, sz, mt)
					}
				end
			else
				sha=calcSha(ap) 

				sha_exists = false
				semaphore.synchronize {
					sha_exists = recorder.exists_sha?(sha)
				}
				if sha_exists
					semaphore.synchronize {
						previous_ap = recorder.getFullpath(sha);
					}

					if File.identical?(ap, previous_ap)
						semaphore.synchronize{
							log.log_skipped(ap, "Identical as #{previous_ap}")
							puts "Is identical to #{previous_ap}"
						}
				
					else
						semaphore.synchronize{
							puts "Recorded as duplicate to #{previous_ap}"
							recorder.insertAsDuplicate(sha, ap)
						}
					end
				else
					semaphore.synchronize{
						puts "Recorded as brand new"
						recorder.insertToDb(sha, ap, sz, mt) 
					}
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
