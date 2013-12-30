require 'redis-objects'
class Bucket
  include Redis::Objects
  attr_reader :bucket_name
  value :worker_pid
  list :jobs

  def initialize name
    @bucket_name = name
  end

  def self.all
    redis.keys("bucket:*").sort.map{|str| Bucket.new(str.split(":")[1])}
  end

  def id
    @bucket_name
  end

  def claim! pid
    self.worker_pid = pid
    key = "bucket:#{id}:worker_pid"
    self.worker_pid = nil unless redis.expire key, 60
    raise "Failed to claim" unless worker_pid
    worker_pid
  end

  def self.for_pid pid
    puts "Checking for_pid: #{pid}"
    claimed_buckets.inject([]) {|buckets_for, bucket|
      buckets_for << bucket if bucket.worker_pid.value.to_s == pid.to_s
      buckets_for
    }
  end

  def self.claimed_bucket_ids
    redis.keys("bucket:*:worker_pid").map{|str| str.split(':')[1]}
  end

  def self.claimed_buckets
    claimed_bucket_ids.map{|did| Bucket.new(did)}
  end

  def self.all_bucket_ids
    redis.keys("bucket:*").map{|str| str.split(':')[1]}
  end

  def self.unclaimed_bucket_ids
    all_bucket_ids - claimed_bucket_ids
  end

  def self.unclaimed_buckets
    unclaimed_bucket_ids.map{|did| Bucket.new(did)}
  end

  def claimed_by? pid
    worker_pid == pid.to_s
  end

  def self.claimed_for pid
    claimed_buckets.delete_if{|bucket|
      !bucket.claimed_by? pid
    }
  end

  def shift
    claim! worker_pid.value unless worker_pid.nil?
    jobs.shift
  end

  def push message
    jobs.push message
  end
end