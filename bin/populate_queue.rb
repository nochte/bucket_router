require 'onstomp'
require 'json'

puts "ARG: #{ARGV}"

raise Exception.new("Arguments required: queue_name, number_buckets, minimum_messages, maximum_messages") unless ARGV.length == 4
queue, buckets, min, max = ARGV

con_str = "stomp://admin:admin@localhost"
clients = [1..25].map{
  sleep 0.01
  OnStomp.connect con_str
}
current_client_index = 0

message_base = '{
    "id": "52af97552e77379e690002fb",
    "type": "reading",
    "timestamp": "2013-12-17T00:13:41Z",
    "data": {
    "gps_latitude": "29.720866666666666",
    "event_category": "66",
    "gps_speed": "31.99166871",
    "gps_number_of_satellites": "9",
    "bucket_type": "enfora",
    "gps_longitude": "-98.66313333333333",
    "event_type": "ignition_on",
    "rtc_timestamp": "2013-12-17T00:13:39.000+0000",
    "event_timestamp_source": "gps_timestamp",
    "raw_payload_size": "71",
    "event_timestamp": "2013-12-17T00:13:40.000+0000",
    "sequence_number": "5477",
    "gps_timestamp": "2013-12-17T00:13:40.000+0000",
    "event_code": "900",
    "event_parse_method": "legacy",
    "gps_fix_type": "1",
    "bucket_name_type": "imei",
    "gps_heading": "234.6",
    "raw_payload": "0x0, 0x5, 0x2, 0xc, 0x6, 0x2, 0x0, 0x29, 0xff, 0xc7, 0x6, 0x3, 0x0, 0x0, 0x15, 0x65, 0x0, 0x0, 0x3, 0x84, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x30, 0x31, 0x32, 0x35, 0x36, 0x33, 0x30, 0x30, 0x30, 0x33, 0x33, 0x39, 0x39, 0x32, 0x32, 0x20, 0x42, 0x2, 0x9c, 0xcd, 0x1, 0x2c, 0xe9, 0x14, 0xff, 0x69, 0xdb, 0x54, 0x1, 0x16, 0x9, 0x2a, 0x0, 0x5, 0x3c, 0x0, 0x1, 0x9d, 0x9, 0xd, 0xc, 0x11, 0x0, 0xd, 0x27",
    "bucket_port": "31250",
    "sms_account_id": "5063",
    "bucket_ip": "172.29.13.252",
    "gps_altitude": "413.0"
},
    "headers": {
    "gateway_timing_service_processed": "2013-12-17T00:13:41.000+0000",
    "sps_common_bucket_routing": "{\"bucket\":{\"id\":692667,\"bucket_name\":\"012563000339922\",\"bucket_name_type\":{\"id\":0,\"name\":\"imei\"},\"account\":null,\"keys\":[],\"hardware_attributes\":[]},\"account\":{\"id\":1454,\"name\":\"RSC Comm_Prod\",\"sms_customer_id\":\"CM-7093\",\"sms_account_id\":\"5063\"},\"account_service_routes\":[{\"id\":1,\"name\":\"Raw feed\",\"queue\":\"gwaas.store-and-forward.in\"}],\"account_delivery_profiles\":[{\"id\":22,\"name\":\"RSCComm Prod\",\"account_default\":true,\"delivery_type\":{\"id\":1,\"name\":\"JMS to ActiveMQ\",\"code\":\"JMS-AMQ\"},\"delivery_format_type\":{\"id\":1,\"name\":\"JSON\",\"code\":\"JSON\"},\"params\":[{\"id\":88,\"name\":\"url\",\"value\":\"tcp://rsccomm2-prod-mq1.numerexfast.com:61616\"},{\"id\":89,\"name\":\"queue\",\"value\":\"telematics_rsccomm\"},{\"id\":90,\"name\":\"username\",\"value\":\"app\"},{\"id\":91,\"name\":\"password\",\"value\":\"smr92wmw3T\"},{\"id\":92,\"name\":\"auth-type\",\"value\":\"user-pass\"}],\"account\":{\"id\":0,\"name\":null,\"sms_customer_id\":null,\"sms_account_id\":\"5063\"}}],\"bucket_service_routes\":[],\"bucket_delivery_profiles\":[]}",
    "gateway_host_name": "gwaas-prod-gw1.numerexfast.com",
    "gwaas_services-router_processed": "2013-12-17T00:13:41.530+0000",
    "gateway_timing_service_action": "use 2 gps_timestamp",
    "gwaas_services-router_routes": "[gwaas.store-and-forward.in, gwaas.message-archive.in]",
    "msg_version": "1"
},
    "message_id": "b20391bd-911b-468b-8ce8-82906d3061a2",
    "app_version": 1
}'

messages = []
(0...buckets.to_i).each do |bucket_index|
  (0..rand(min.to_i..max.to_i)).each do
    puts "Bucket_name: #{bucket_index}"
    msg = JSON.parse message_base
    msg['id'] = (('a'..'g').to_a + ('A'..'G').to_a + ((0..9).to_a)*10).shuffle[0, 32].join
    unless rand(1..100) == 50
      msg['data']['bucket_name'] = "bucket_#{bucket_index}" unless rand(1..100) == 50
    end
    messages << msg
  end
end

messages.shuffle!

puts "Messages count: #{messages.count}"
puts "Message IDs: #{messages.map{|m| m['id']}.uniq.count}"
puts "Message_bucket_names: #{messages.map{|m| m['data']['bucket_name']}}"

messages.each do |m|
  puts "Sending: #{m['data']['bucket_name']}"
  client = clients[current_client_index]
  client.send queue, m.to_json
  current_client_index++
  current_client_index = 0 if current_client_index < clients.count
end

sleep 60 * 10