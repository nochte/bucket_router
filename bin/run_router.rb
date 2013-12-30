require 'bucket_job_router'

ENV['APP_ENV'] = 'develop' unless ENV['APP_ENV']

#here, we're going to read a config file to determine
#  things like what worker class (a symbol) to load

router = BucketJobRouter::Worker.new({ :router => true })
while 1
  sleep 1
end

#
#puts "Registering some messages"
#
#rand(10..50).times do |did|
#  d = Bucket.new(did)
#  rand(50..1000).times do |iter|
#    d.messages << iter
#  end
#end
#
#Bucket.all.each do |bucket|
#  puts "Bucket currently has #{bucket.messages.length} messages"
#end
#
#Bucket.all.each do |bucket|
#  bucket.messages.length.times do
#    puts "#{bucket.id} :: Dequing: #{bucket.messages.pop}"
#  end
#end