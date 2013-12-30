require 'bucket_job_router/version'
require 'redis-objects'
require 'redis_monkey_patch'
require 'message_router'
require 'models/bucket'

module BucketJobRouter
  class Worker < Message::Worker::Base
    subscribes_to 'bucket_test'

    #OVERRIDE PATH_TO_BUCKET
    #PATH_TO_BUCKET points to a nested element inside of the message
    #  which indicates routing. for example:
    #  ['data', 'bucket_name'] points to message['data']['bucket_name']
    PATH_TO_BUCKET = ['data', 'bucket_name']

    #OVERRIDE PROCESS_JOB
    def process_job message
      message = JSON.parse message
      @count ||= 0
      @count += 1
      puts "#{Process.pid} :: #{@count} :: Processing: #{message['id']}"
      sleep rand(0.04..0.25)
    end

    def worker_queue_attributes message = nil
      queue_stats = {:enqueue => :push, :dequeue => :shift}
      if @nature == :worker
        #here, we pick a bucket to pull from and present it to the queue stats
        #if we don't have a current bucket, get the first unclaimed bucket
        #if we do have a current bucket, check to see if it is not empty, select it
        #  else, find the current bucket's index in
        if @current_bucket.nil? || @current_bucket.jobs.length <= 0
          claimed_buckets = Bucket.for_pid Process.pid
          current_bucket_index = claimed_buckets.index(@current_bucket) || -1
          @current_bucket = nil #we're looking for a new one here, so let's jack out
          claimed_buckets.push *claimed_buckets.slice!(0..current_bucket_index)
          claimed_buckets.each do |bucket|
            bucket.claim! Process.pid #refreshes the lock
            @current_bucket = bucket if bucket.jobs.length > 0 #only visit buckets that have jobs to work
          end
          unless @current_bucket
            unclaimed_buckets = Bucket.unclaimed_buckets
            unclaimed_buckets.each do |bucket|
              @current_bucket = bucket if bucket.jobs.length > 0
              break unless @current_bucket.nil?
            end
            @current_bucket = Bucket.new("-") unless @current_bucket #no bucket
          end
          @current_bucket.claim! Process.pid
        end
        queue_stats[:queue] = @current_bucket
      else
        bucket_id = self.class.bucket_id_for message
        bucket = Bucket.new(bucket_id.to_s)
        queue_stats[:queue] = bucket
      end
      queue_stats
    end

    #override this method!
    def self.bucket_id_for message
      PATH_TO_BUCKET.inject(message) {|val, key|
        val.send(:[], key) rescue 'unbucketted'
      }
    end

    #DON'T OVERRIDE THIS METHOD
    def reconnect
      log :info, "Connecting to Redis"
      @redis = Bucket.redis
      @redis.client.reconnect
    end
  end
end
