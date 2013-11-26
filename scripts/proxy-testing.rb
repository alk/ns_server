#!/usr/bin/env ruby

require 'socket'
require 'json'

BIND_PORT = ARGV[0].to_i

PROXY_HOST = ARGV[1]
PROXY_PORT = ARGV[2].to_i

PROXY2_HOST = ARGV[3]
PROXY2_PORT = ARGV[4].to_i

HOST = ARGV[5]
PORT = ARGV[6].to_i

def run_accept_loop(server_sock)
  while true
    client = server_sock.accept
    puts "got client #{client}"
    Thread.new { run_client_loop(client) }
  end
end

class IO
  def send_payload(payload)
    pj = payload.to_json
    pj.force_encoding!(Encoding::ASCII_8BIT)
    tosend = [pj.size].pack("N") + pj
    self.write tosend
  end
  def recv_payload
    size = self.read(4)
    size.force_encoding!(Encoding::ASCII_8BIT)
    size = size.unpack("N")[0]
    JSON.parse(self.read(size))
  end
end

def run_pipe_loop(from, to)
  while true
    data = begin
             from.readpartial(16384)
           rescue EOFError
             return
           end
    to.write data
  end
end

def run_client_loop(client)
  puts "starting client loop: #{client}"
  client2 = if ENV['IGNORE_PROXY']
              TCPSocket.new(HOST, PORT)
            else
              client2 = TCPSocket.new(PROXY_HOST, PROXY_PORT)
              client2.send_payload({:host => HOST, :port => PORT,
                                     :proxyHost => PROXY2_HOST,
                                     :proxyPort => PROXY2_PORT})
              reply = client2.recv_payload
              if reply["type"] != "ok"
                puts "failed to establish proxy connection: #{reply.inspect}"
                raise "failed to establish proxy connection: #{reply.inspect}"
              end
            end
  upstream_sender = Thread.new {run_pipe_loop(client, client2)}
  downstream_sender = Thread.new {run_pipe_loop(client2, client)}
  begin
    upstream_sender.join
    downstream_sender.join
  ensure
    client.close rescue nil
    client2.client2 rescue nil
    upstream_sender.join rescue nil
    downstream_sender.join rescue nil
  end
rescue Exception => exc
  client.close rescue nil
  puts "got exception: #{exc}\n#{exc.backtrace.join("\n")}"
end

run_accept_loop(TCPServer.new(BIND_PORT))
