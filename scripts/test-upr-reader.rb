#!/usr/bin/ruby

UprRequest = Struct.new(:opcode, :datatype, :vbucket, :opaque, :cas, :ext, :key, :body)

class IO
  def read_full(size)
    rv = ""
    while size > 0
      chunk = self.read(size)
      raise "premature EOF" unless chunk
      rv << chunk
      size -= chunk.bytesize
    end
    rv
  end
end

module MCStatus
  # Response status codes.
  SUCCESS          = 0x00
  KEY_ENOENT       = 0x01
  KEY_EEXISTS      = 0x02
  E2BIG            = 0x03
  EINVAL           = 0x04
  NOT_STORED       = 0x05
  DELTA_BADVAL     = 0x06
  NOT_MY_VBUCKET   = 0x07
  MC_AUTH_ERROR    = 0x20
  MC_AUTH_CONTINUE = 0x21
  ERANGE           = 0x22
  ROLLBACK         = 0x23
  UNKNOWN_COMMAND  = 0x81
  ENOMEM           = 0x82
  NOT_SUPPORTED    = 0x83
  EINTERNAL        = 0x84
  EBUSY            = 0x85
  ETMPFAIL         = 0x86
end

class UprRequest
  def status
    self.vbucket
  end

  HDR_PACK = ("CCS>"+           # magic opcode keylen
              "CCS>"+           # extlen datatype vbucket
              "L>"+             # bodylen
              "L>"+             # opaque
              "Q>")             # cas

  UPR_OPEN =                  0x50
  UPR_ADD_STREAM =            0x51
  UPR_CLOSE_STREAM =          0x52
  UPR_STREAM_REQ =            0x53
  UPR_GET_FAILOVER_LOG =      0x54
  UPR_STREAM_END =            0x55
  UPR_SNAPSHOT_MARKER =       0x56
  UPR_MUTATION =              0x57
  UPR_DELETION =              0x58
  UPR_EXPIRATION =            0x59
  UPR_FLUSH =                 0x5a
  UPR_SET_VBUCKET_STATE =     0x5b
  UPR_NOP =                   0x5c
  UPR_WINDOW_UPDATE =         0x5d
  UPR_CONTROL =               0x5e

  attr_accessor :magic

  def pack
    opcode = self.opcode || raise
    datatype = self.datatype || 0
    vbucket = self.vbucket || 0
    opaque = self.opaque || 0
    cas = self.cas || 0
    ext = self.ext || ""
    key = self.key || ""
    body = self.body || ""

    keylen = key.bytesize
    extlen = ext.bytesize
    bodylen = keylen + extlen + body.bytesize

    [128, opcode, keylen,
     extlen, datatype, vbucket,
     bodylen,
     opaque,
     cas].pack(HDR_PACK) + ext + key + body
  end

  def self.read(io)
    (magic, opcode, keylen,
     extlen, datatype, vbucket,
     bodylen,
     opaque,
     cas) = hdr = io.read_full(24).unpack(HDR_PACK)
    # p hdr
    fullbody = io.read_full(bodylen).force_encoding(Encoding::ASCII_8BIT)
    raise "premature eof #{fullbody.inspect} #{bodylen} #{fullbody.bytesize}" unless fullbody.bytesize == bodylen
    ext = fullbody[0...extlen]
    key = fullbody[extlen...(extlen+keylen)]
    body = fullbody[(keylen+extlen)..-1]
    rv = self.new(opcode, datatype, vbucket, opaque, cas, ext, key, body)
    rv.magic = magic
    rv
  end

  def self.from_hash(hash)
    rv = self.new
    rv.members.each do |k|
      rv[k] = hash[k]
    end
    rv
  end
end

require 'socket'

s = TCPSocket.new("127.0.0.1", 11999)

r = UprRequest.from_hash(:opcode => UprRequest::UPR_OPEN,
                         :key => "asdasd",
                         :ext => [1].pack("Q>")) # producer

# p r

# IO.popen("hd", "w") {|f| f << r.pack}

s << r.pack

v = UprRequest.read(s)

p v

raise unless v.opcode == UprRequest::UPR_OPEN && v.magic == 129 && v.status == 0

# build_stream_request_packet(Vb, Opaque, StartSeqno, EndSeqno, VBUUID, VBUUIDSeqno) ->
#     Extra = <<0:64, StartSeqno:64, EndSeqno:64, VBUUID:64, VBUUIDSeqno:64>>,
#     #upr_packet{opcode = ?UPR_STREAM_REQ,
#                 vbucket = Vb,
#                 opaque = Opaque,
#                 ext = Extra}.

def build_stream_request_packet(vb, opaque, start_seqno, end_seqno, vbuuid, vbuuidseqno)
  ext = [0, start_seqno, end_seqno, vbuuid, vbuuidseqno].pack("Q>Q>Q>Q>Q>")
  UprRequest.from_hash(:opcode => UprRequest::UPR_STREAM_REQ,
                       :opaque => 0xafafafaf,
                       :ext => ext)
end

r = build_stream_request_packet(0, 0xafafafaf, 0, 10000000, 0x123123, 0)

t_start = Time.now

s << r.pack

sleep(1)

v = UprRequest.read(s)

p v

raise unless v.opcode == UprRequest::UPR_STREAM_REQ && v.magic == 129 && v.status == 0

str = ""

seen = 0

trap(:INT) {puts "seen: #{seen}, time: #{Time.now.to_f - t_start.to_f}"; exit}

while s.readpartial(1024*1024, str)
  old_seen = seen
  seen += str.bytesize
  if (old_seen >> 18) != (seen >> 18)
    # print "."; STDOUT.flush
    STDOUT.syswrite(".")
  end
end

