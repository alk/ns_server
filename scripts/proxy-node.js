#!/usr/bin/env node

var util = require("util");
var fs = require('fs');
var tls = require('tls');
var net = require('net');

;(function () {
  var cmd = "";
  process.stdin.resume();
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", function (data) {
    cmd = cmd + data;
    if (cmd.indexOf("\n") != -1) {
      console.log("got cmd: ", cmd);
      console.log("but we can only exit anyway. Thus exiting");
      // TODO: consider flushing everything before exiting
      process.exit(0);
    }
  });
 })();

console.log("ready");

var proxyOptions = {
  tlsPort: 9999,
  plainPort: 9998
};

;(function setupTSLServer() {
  var options = {
    key: fs.readFileSync('/root/src/test/go-ssh-bench/key.pem'),
    cert: fs.readFileSync('/root/src/test/go-ssh-bench/cert.pem'),
    halfOpen: true
  };
  tls.createServer(options, handleTLSConnection).listen(proxyOptions.tlsPort);
})();

;(function setupPlainServer() {
  net.createServer({halfOpen: true}, handlePlainConnection).listen(proxyOptions.plainPort);
})();

function onceOrError(emitter, eventName, callback) {
  function onError(err) {
    emitter.removeListener(eventName, cb);
    callback(err);
  }
  function onEvent() {
    emitter.removeListener("error", onError);
    var args = Array.prototype.concat.apply([null], arguments);
    callback.apply(null, args);
  }
  emitter.once(eventName, onEvent);
  emitter.once("error", onError);
}

function readGivenLength(c, data, wantedLength, callback) {
  onceOrError(c, "data", function (err, moreData) {
    if (err) {
      return callback(err);
    }
    var nowData = Buffer.concat([data, moreData]);
    if (nowData.length < wantedLength) {
      readGivenLength(c, nowData, wantedLength, callback);
      return;
    }
    callback(null, nowData.slice(0, wantedLength), nowData.slice(wantedLength));
  });
}

function readPayloadSize(c, data, callback) {
  readGivenLength(c, data, 4, function (err, lengthBuf, rest) {
    if (err) {
      callback(err);
      return;
    }
    callback(null, lengthBuf.readUInt32BE(0), rest);
  });
}

function tcpConnect(host, port, callback) {
  var c = net.createConnection(host, port, {halfOpen: true});
  onceOrError(c, "connected", callback);
}


function sendPayload(socket, payload, callback, isEnd) {
  var b1 = new Buffer(JSON.stringify(payload));
  var b0 = new Buffer(4);
  b0.writeUInt32BE(b1.length, 0);
  var b = Buffer.concat([b0, b1]);
  if (isEnd) {
    socket.end(b);
    onceOrError(socket, "finish", callback);
  } else {
    socket.write(b);
    onceOrError(socket, "drain", callback);
  }
}

function closeNoError(obj) {
  try {
    obj.close();
  } catch (e) {}
}

function doPayloadHanshake(c, successCallback, maybeErrorCallback) {
  if (!maybeErrorCallback) {
    maybeErrorCallback = function (err, place) {
      onHandshakeError(c, err, place);
    }
  }
  readPayloadSize(c, new Buffer(0), function (err, payloadSize, data) {
    if (err) {
      return maybeErrorCallback(err, "payloadSize");
    }
    readGivenLength(c, data, payloadSize, function (err, payload, rest) {
      if (err) {
        return maybeErrorCallback(err, "payload");
      }
      c.pause();
      c.unshift(rest);
      var parsedPayload;
      try {
        parsedPayload = JSON.parse(payload.toString());
      } catch (parseErr) {
        return maybeErrorCallback(parseErr, "payload-parse");
      }
      successCallback(parsedPayload);
    });
  });
}

function handleTLSConnection(c) {
  console.log("got ssl connection: ", c);
  doPayloadHanshake(c, function (parsedPayload) {
    var host = parsedPayload["host"];
    var port = parsedPayload["port"];
    if (!host || !port) {
      return onHandshakeError(c, "bad payload", "payload-parse");
    }

    connectTLSToPlain(c, host, port);
  });
}

function onHandshakeError(c, err, place) {
  console.log("got handshake error: ", err, "at ", place, ". Closing ", c);
  closeNoError(c);
}

function connectTLSToPlain(upstream, host, port) {
  tcpConnect(host, port, function (err, downstream) {
    if (err) {
      return sendPayloadAndClose(upstream, {type: "downstreamConnectError", err: util.inspect(err)}, downstream);
    }

    downstream.pause();

    sendPayload(upstream, {type: "ok"}, function (err) {
      if (err) {
        return onTLSError.call(upstream, err);
      }

      upstream.downstream = downstream;
      downstream.upstream = upstream;
      downstream.on("error", onTLSDownstreamError.bind(downstream));
      upstream.on("error", onTLSError.bind(c));

      downstream.resume();
      upstream.resume();
      upstream.pipe(downstream);
      downstream.pipe(upstream);
    });
  });
}

function onTLSError(err) {
  console.log("got error on tls connection: ", this, ": ", err);
  closeNoError(this);
  closeNoError(this.downstream);
}

function onTLSDownstreamError(err) {
  console.log("got error on downstream connection: ", this, ": ", err);
  closeNoError(this);
  closeNoError(this.upstream);
}

function sendPayloadAndClose(socket, payload) {
  var args = arguments;
  return sendPayload(socket, payload, afterSendDisconnect, true);
  function afterSendDisconnect() {
    closeNoError(upstream);
    var i = args.length - 1;
    for (;i > 1;i--) {
      closeNoError(arguments[i]);
    }
  }
}

function handlePlainConnection(upstream) {
  console.log("got plain connection: ", c);
  doPayloadHanshake(upstream, function (parsedPayload) {
    var proxyHost = parsedPayload["proxyHost"];
    var proxyPort = parsedPayload["proxyPort"];
    var host = parsedPayload["host"];
    var port = parsedPayload["port"];
    var cert = parsedPayload["cert"];

    if (!proxyHost || !proxyPort || !host || !port || !cert) {
      return sendPayloadAndClose(upstream, {type: "badPayload", payload: parsedPayload});
    }

    var downstream = tls.connect({host: proxyHost, port: proxyPort});
    onceOrError(downstream, "secureConnect", function (err) {
      if (err) {
        return sendPayloadAndClose(upstream, {type: "downstreamConnectError", err: util.inspect(err)});
      }

      // TODO: check cert
      handlePlainWithDownstream(upstream, downstream, host, port);
    });
  });
}

function handlePlainWithDownstream(upstream, downstream, host, port) {
  sendPayload(downstream, {host: host, port: port}, function (err) {
    if (err) {
      var errPayload = {type: "downstreamHandshakeSendError",
                        err: util.inspect(err)};
      return sendPayloadAndClose(upstream, errPayload, downstream);
    }

    return doPayloadHanshake(downstream, onSuccess, onError);
    function onSuccess(reply) {
      if (reply["type"] == "ok") {
        return runPlainSide(upstream, downstream);
      }
      sendPayloadAndClose(upstream, reply, downstream);
    }
    function onError(err, place) {
      var errPayload = {type: "downstreamHandshakeRecvError",
                        err: util.inspect(err),
                        place: place};
      sendPayloadAndClose(upstream, errPayload, downstream);
    }
  });
}

function runPlainSide(upstream, downstream) {
  sendPayload(upstream, {type: "ok"}, function (err) {
    if (err) {
      console.log("failed to send back ack to plain upstream: ", err);
      closeNoError(upstream);
      closeNoError(downstream);
      return;
    }

    upstream.downstream = downstream;
    downstream.upstream = upstream;
    upstream.on("error", onPlainError.bind(upstream));
    downstream.on("error", onPlainDownsteamError.bind(downstream));

    upstream.resume();
    downstream.resume();
    downstream.pipe(upstream);
    upstream.pipe(downstream);
  });
}

function onPlainError(err) {
  console.log("got error on plain connection: ", this, ": ", err);
  closeNoError(this);
  closeNoError(this.downstream);
}

function onPlainDownsteamError(err) {
  console.log("got error on plain downstream connection: ", this, ": ", err);
  closeNoError(this);
  closeNoError(this.upstream);
}
