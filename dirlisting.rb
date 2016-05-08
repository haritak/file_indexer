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

def thread_says(id, msg)
	puts "#{id}: #{msg}"
end

$MAX_NO_THREADS = 50
$MIN_NO_THREADS = 25
$MI = true #minimal input

semaphore = Mutex.new
Thread.abort_on_exception = true

begin
	puts "Initializing"
	log = Logger.new
	recorder = Recorder.new
	puts "Initialized"

	@start = Time.now

	puts "Getting directory listing..."

	Dir.glob("**/*").each_with_index do |fn,i|

	  #this part is executing by a single thread only

	  puts "Started" if (i==1) 

	  local_fn = ''
	  semaphore.synchronize {
		  local_fn = fn
	  }
	

	  while Thread.list.count > $MAX_NO_THREADS do
		puts "While working for file #{i}, I am waiting for some threads to finish"
		STDOUT.flush
		STDERR.flush
		while Thread.list.count > $MIN_NO_THREADS do
			puts Thread.list.count
			sleep 1
			puts Thread.list.count
			sleep 1
		end
	  end

	  Thread.new do
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

			mid = Thread.current.object_id
			semaphore.synchronize {
				thread_says mid, "----processing file number #{i} ----" unless $mi
				thread_says mid, ap unless $mi
			}

			sz = File.size(ap);
			mt = File.mtime(ap).to_f;

			thread_says mid, "#{sz} #{mt}" unless $mi

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
					thread_says mid, "File was found with size #{p_sz} and modification time #{p_mt}" unless $mi
				}
				
				
				if p_sz == sz && p_mt == mt
					semaphore.synchronize{
						log.log_skipped(ap, "same size, same mt already recorded")
						thread_says mid, "same size, same mod time, skipped"
					}
				else
					sha = calcSha(ap)
					semaphore.synchronize {
						thread_says mid, "Record updated due to file contents change"
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
					previous_ap=''
					semaphore.synchronize {
						previous_ap = recorder.getFullpath(sha);
					}

					if File.identical?(ap, previous_ap)
						semaphore.synchronize{
							log.log_skipped(ap, "Identical as #{previous_ap}")
							thread_says mid, "Is identical(same inode) to #{previous_ap}"
						}
				
					else
						semaphore.synchronize{
							thread_says mid, "Recorded as duplicate to #{previous_ap}"
							recorder.insertAsDuplicate(sha, ap)
						}
					end
				else
					semaphore.synchronize{
						thread_says mid, "Recorded as brand new"
						recorder.insertToDb(sha, ap, sz, mt) 
					}
				end
			end
		end

		Thread.exit
	  end

	end

	sleep 10
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
