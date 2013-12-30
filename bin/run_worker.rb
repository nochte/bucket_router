#!/usr/bin/env ruby

require "./lib/bucket_job_router"

#here, we're going to read a config file to determine
#  things like what worker class (a symbol) to load

worker = BucketJobRouter::Worker.new
worker.setup_worker
worker.run_worker

