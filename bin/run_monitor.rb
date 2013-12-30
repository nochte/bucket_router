#Purpose is to show the following:
# number of jobs exist for each bucket
# which buckets are claimed
# which processes own each bucket

require './lib/models/bucket'

while 1
  job_counts = {'total' => 0}
  claims = {}
  Bucket.all.each do |bucket|
    job_counts[bucket.id] = bucket.jobs.count
    job_counts['total'] += bucket.jobs.count
    unless bucket.worker_pid.value.nil?
      claims[bucket.worker_pid.value] ||= []
      claims[bucket.worker_pid.value] << bucket.id rescue nil
    end
  end

  job_counts.each do |bid, val|
    puts "#{bid} :: #{val}"
  end

  puts "Total: #{job_counts['total']}"

  claims.each do |cl, buckets|
    puts "#{cl} : (#{buckets.uniq.count})"
  end
  sleep 1
end