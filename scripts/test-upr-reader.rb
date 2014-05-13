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
    p hdr
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

p r

IO.popen("hd", "w") {|f| f << r.pack}

s << r.pack

# build_stream_request_packet(Vb, Opaque, StartSeqno, EndSeqno, VBUUID, VBUUIDSeqno) ->
#     Extra = <<0:64, StartSeqno:64, EndSeqno:64, VBUUID:64, VBUUIDSeqno:64>>,
#     #upr_packet{opcode = ?UPR_STREAM_REQ,
#                 vbucket = Vb,
#                 opaque = Opaque,
#                 ext = Extra}.


v = UprRequest.read(s)

p v
