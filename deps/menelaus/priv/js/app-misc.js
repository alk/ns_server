function setBoolAttribute(jq, attr, value) {
  if (value) {
    jq.attr(attr, attr);
  } else {
    jq.removeAttr(attr);
  }
}

function normalizeNaN(possNaN) {
  return possNaN << 0;
}

function setFormValues(form, values) {
  form.find('input[type=text], input[type=password], input:not([type])').each(function () {
    var text = $(this);
    var name = text.attr('name');
    var value = String(values[name] || '');
    text.val(value);
  });

  form.find('input[type=checkbox]').each(function () {
    var box = $(this);
    var name = box.attr('name');
    if (!(name in values))
      return;

    var boolValue = values[name];
    if (_.isString(boolValue)) {
      boolValue = (boolValue != "0");
    }

    setBoolAttribute(box, 'checked', boolValue);
  });

  form.find('input[type=radio]').each(function () {
    var box = $(this);
    var name = box.attr('name');
    if (!(name in values))
      return;

    var boolValue = (values[name] == box.attr('value'));
    setBoolAttribute(box, 'checked', boolValue);
  });

  form.find("select").each(function () {
    var select = $(this);
    var name = select.attr('name');
    if (!(name in values))
      return;

    var value = values[name];

    select.find('option').each(function () {
      var option = $(this);
      setBoolAttribute($(this), 'selected', option.val() == value);
    });
  });
}

function formatUptime(seconds, precision) {
  precision = precision || 8;

  var arr = [[86400, "days", "day"],
             [3600, "hours", "hour"],
             [60, "minutes", "minute"],
             [1, "seconds", "second"]];

  var rv = [];

  $.each(arr, function () {
    var period = this[0];
    var value = (seconds / period) >> 0;
    seconds -= value * period;
    if (value)
      rv.push(String(value) + ' ' + (value > 1 ? this[1] : this[2]));
    return !!--precision;
  });

  return rv.join(', ');
}

function postWithValidationErrors(url, data, callback, ajaxOptions) {
  if (!_.isString(data))
    data = serializeForm(data);
  var finalAjaxOptions = {
    type:'POST',
    url: url,
    data: data,
    success: continuation,
    error: continuation,
    dataType: 'json'
  };
  _.extend(finalAjaxOptions, ajaxOptions || {});
  var action = new ModalAction();
  $.ajax(finalAjaxOptions);
  return

  function continuation(data, textStatus) {
    action.finish();
    if (textStatus == 'success') {
      return callback.call(this, data, textStatus);
    }

    var status = 0;
    try {
      status = data.status // can raise exception on IE sometimes
    } catch (e) {
      // ignore
    }

    if (status >= 200 && status < 300 && data.responseText == '') {
      return callback.call(this, '', 'success');
    }

    var errorsData;

    if (textStatus == 'timeout') {
      errorsData = "Save request failed because of timeout.";
    } else if (status == 0) {
      errorsData = "Got no response from save request.";
    } else if (status != 400 || textStatus != 'error') {
      errorsData = "Save request returned error.";
    } else {
      errorsData = $.httpData(data, null, this);
    }

    if (!_.isArray(errorsData)) {
      if (errorsData == null)
        errorsData = "unknown reason";
      errorsData = [errorsData];
    }
    callback.call(this, errorsData, 'error');
  }
}

function runFormDialog(uriOrPoster, dialogID, options) {
  options = options || {};
  var dialogQ = $('#' + dialogID);
  var form = dialogQ.find('form');
  var response = false;

  var errors = dialogQ.find('.errors');
  errors.hide();

  var poster;
  if (_.isString(uriOrPoster))
    poster = _.bind(postWithValidationErrors, null, uriOrPoster);
  else
    poster = uriOrPoster;

  function callback(data, status) {
    if (status == 'success') {
      response = data;
      hideDialog(dialogID);
      return;
    }

    if (!errors.length) {
      alert('submit failed: ' + data.join(' and '));
      return;
    }
    errors.html();
    _.each(data, function (message) {
      var li = $('<li></li>');
      li.text(message);
      errors.append(li);
    });
    errors.show();
  }

  function onSubmit(e) {
    e.preventDefault();
    if (options.validate) {
      var errors = options.validate();
      if (errors && errors.length) {
        callback(errors, 'error');
        return;
      }
    }
    poster(form, callback);
  }

  form.bind('submit', onSubmit);
  setFormValues(form, options.initialValues || {});
  showDialog(dialogID, {
    onHide: function () {
      form.unbind('submit', onSubmit);
      if (options.closeCallback) {
        options.closeCallback(response);
      }
    }
  });
}

// make sure around 3 digits of value is visible. Less for for too
// small numbers
function truncateTo3Digits(value, leastScale) {
  var scale = _.detect([100, 10, 1, 0.1, 0.01, 0.001], function (v) {return value >= v;}) || 0.0001;
  if (leastScale != undefined && leastScale > scale)
    scale = leastScale;
  scale = 100 / scale;
  return Math.floor(value*scale)/scale;
}

function prepareTemplateForCell(templateName, cell) {
  cell.undefinedSlot.subscribeWithSlave(function () {
    prepareRenderTemplate(templateName);
  });
  if (cell.value === undefined)
    prepareRenderTemplate(templateName);
}

function mkCellRenderer(to, options, cell) {
  var template;

  if (_.isArray(to)) {
    template = to[1] + '_template';
    to = to[0];
  } else {
    template = to + "_template";
    to += '_container'
  }

  var toGetter;
  if (_.isString(to)) {
    toGetter = function () {
      return $i(to);
    }
  } else {
    toGetter = function () {
      return to;
    }
  }

  options = options || {};

  return function () {
    if (options.hideIf) {
      if (options.hideIf(cell)) {
        $(toGetter()).hide();
        return;
      }
      $(toGetter()).show();
    }

    var value = cell.value;
    if (value == undefined) {
      return prepareAreaUpdate($(toGetter()));
    }

    if (options.valueTransformer)
      value = (options.valueTransformer)(value);

    if (value === cell.value) {
      value = _.clone(value);
    }

    if (options.beforeRendering)
      (options.beforeRendering)(cell);
    renderRawTemplate(toGetter(), template, value);
  }
}

// renderCellTemplate(cell, "something");
// renderCellTemplate(cell, ["something_container", "foorbar"]);
function renderCellTemplate(cell, to, options) {
  var slave = new Slave(mkCellRenderer(to, options, cell));
  cell.changedSlot.subscribeWithSlave(slave);
  cell.undefinedSlot.subscribeWithSlave(slave);
  slave.thunk(cell);

  var extraCells = (options || {}).extraCells || [];

  _.each(extraCells, function (cell) {
    cell.changedSlot.subscribeWithSlave(slave);
    cell.undefinedSlot.subscribeWithSlave(slave);
  });

  return {
    cancel: function () {
      cell.changedSlot.unsubscribe(slave);
      cell.undefinedSlot.unsubscribe(slave);

      _.each(extraCells, function (cell) {
        cell.changedSlot.unsubscribe(slave);
        cell.undefinedSlot.unsubscribe(slave);
      });
    }
  }
}

_.extend(ViewHelpers, {
  thisElement: function (body) {
    var id = _.uniqueId("thisElement");

    AfterTemplateHooks.push(function () {
      var marker = $($i(id));
      var element = marker.parent();
      marker.remove();

      body.call(element.get(0), element);
    });

    return ["<span id='", id, "'></span>"].join('');
  },

  // assigns $.data on current element
  // use with {%= %} !
  setData: function (name, value) {
    return this.thisElement(function (thisElement) {
      $.data(thisElement.get(0), name, value);
    });
  },

  setPercentBar: function (percents) {
    return this.thisElement(function (q) {
      percents = (percents << 0); // coerces NaN and infinities to 0
      q.find('.used').css('width', String(percents)+'%')
    });
  },
  setAttribute: function (name, value) {
    return this.thisElement(function (q) {
      q.attr(name, value);
    });
  },
  specialPluralizations: {
    'copy': 'copies'
  },
  count: function (count, text) {
    if (count == null)
      return '?' + text + '(s)';
    count = Number(count);
    if (count > 1) {
      var lastWord = text.split(/\s+/).slice(-1)[0];
      var specialCase = ViewHelpers.specialPluralizations[lastWord];
      if (specialCase)
        text = specialCase;
      else
        text += 's';
    }
    return [String(count), ' ', text].join('')
  },
  renderHealthClass: function (status) {
    if (status == "healthy")
      return "up";
    else
      return "down";
  },
  formatLogTStamp: function (ts) {
    return window.formatLogTStamp(ts);
  },
  prepareQuantity: function (value, K) {
    K = K || 1024;
    var M = K*K;
    var G = M*K;
    var T = G*K;

    var t = _.detect([[T,'T'],[G,'G'],[M,'M'],[K,'K']], function (t) {return value > 1.1*t[0]});
    t = t || [1, ''];
    return t;
  },
  formatQuantity: function (value, kind, K, spacing) {
    if (spacing == null)
      spacing = '';
    if (kind == null)
      kind = 'B'; //bytes is default

    var t = ViewHelpers.prepareQuantity(value, K);
    return [truncateTo3Digits(value/t[0]), spacing, t[1], kind].join('');
  },
  formatMemSize: function (value) {
    return this.formatQuantity(value, 'B', 1024, ' ');
  },

  renderPendingStatus: function (node) {
    if (node.clusterMembership == 'inactiveFailed') {
      if (node.pendingEject) {
        return "PENDING EJECT FAILED OVER"
      } else {
        return "FAILED OVER";
      }
    }
    if (node.pendingEject) {
      return "PENDING EJECT";
    }
    if (node.clusterMembership == 'active')
      return '';
    if (node.clusterMembership == 'inactiveAdded') {
      return 'PENDING ADD';
    }
    debugger
    throw new Error('cannot reach');
  },

  ifNull: function (value, replacement) {
    if (value == null || value == '')
      return replacement;
    return value;
  },

  stripPort: (function () {
    var cachedAllServers;
    var cachedHostnamesCount;
    return function(value, allServers) {
      var counts;
      if (allServers === undefined || cachedAllServers === allServers) {
        counts = cachedHostnamesCount;
      } else {
        var hostnames = _.map(allServers, function (s) {return s.hostname});
        counts = {};
        var len = hostnames.length;
        for (var i = 0; i < len; i++) {
          var h = hostnames[i].split(":",1)[0];
          if (counts[h] === undefined)
            counts[h] = 1;
          else
            counts[h]++;
        }
        cachedAllServers = allServers;
        cachedHostnamesCount = counts;
      }
      var strippedValue = value.split(":",1)[0];
      if (counts[strippedValue] < 2)
        value = strippedValue;
      return escapeHTML(value)
    }
  })()
});

function genericDialog(options) {
  options = _.extend({buttons: {ok: true,
                                cancel: true},
                      modal: true,
                      fixed: true,
                      callback: function () {
                        instance.close();
                      }},
                     options);
  var text = options.text || 'No text.';
  var header = options.header || 'No Header';
  var dialogTemplate = $('#generic_dialog');
  var dialog = $('<div></div>');
  dialog.attr('class', dialogTemplate.attr('class'));
  dialog.attr('id', _.uniqueId('generic_dialog_'));
  dialogTemplate.after(dialog);
  dialog.html(dialogTemplate.html());

  dialogTemplate = null;

  function brIfy(text) {
    return _.map(text.split("\n"), escapeHTML).join("<br>");
  }

  dialog.find('.lbox_header').html(options.headerHTML || brIfy(header));
  dialog.find('.dialog-text').html(options.textHTML || brIfy(text));

  var b = options.buttons;
  if (!b.ok && !b.cancel) {
    dialog.find('.save_cancel').hide();
  } else {
    dialog.find('.save_cancel').show();
    var ok = b.ok;
    var cancel = b.cancel;

    if (ok === true)
      ok = 'OK';
    if (cancel === true)
      cancel == 'CANCEL';

    function bind(jq, on, name) {
      jq[on ? 'show' : 'hide']();
      if (on) {
        jq.bind('click', function (e) {
          e.preventDefault();
          options.callback.call(this, e, name, instance);
        });
      }
    }
    bind(dialog.find(".save_button"), ok, 'ok');
    bind(dialog.find('.cancel_button'), cancel, 'cancel');
  }

  var modal = options.modal ? new ModalAction() : null;

  showDialog(dialog, {
    onHide: function () {
      _.defer(function () {
        dialog.remove();
      });
    },
    fixed: options.fixed
  });

  var instance = {
    dialog: dialog,
    close: function () {
      if (modal)
        modal.finish();
      hideDialog(dialog);
    }
  };

  return instance;
}

function postClientErrorReport(text) {
  function ignore() {}
  $.ajax({type: 'POST',
          url: "/logClientError",
          data: text,
          success: ignore,
          error: ignore});
}

var originalOnError;
(function () {
  var sentReports = 0;
  var ErrorReportsLimit = 8;
  originalOnError = window.onerror;

  function appOnError(message, fileName, lineNo) {
    var report = [];
    if (++sentReports < ErrorReportsLimit) {
      report.push("Got unhandled error: ", message, "\nAt: ", fileName, ":", lineNo, "\n");
      var bt = collectBacktraceViaCaller();
      if (bt) {
        report.push("Backtrace:\n", bt);
      }
      if (sentReports == ErrorReportsLimit - 1) {
        report.push("Further reports will be suppressed\n")
      }
    }

    // mozilla can report errors in some cases when user leaves current page
    // so delay report sending
    _.delay(function () {
      postClientErrorReport(report.join(''));
    }, 500);

    if (originalOnError)
      originalOnError.call(window, message, fileName, lineNo);
  }
  window.onerror = appOnError;
})();

// clicks to links with href of '#<param>=' will be
// intercepted. Default action (navigating) will be prevented and body
// will be executed.
//
// Middle-clicks that open link in new tab/window will not be (and
// cannot be) intercepted
//
// We use this function to preserve other state that may be in url
// hash string in normal case, while still supporting middle-clicking.
function watchHashParamLinks(param, body) {
  param = '#' + param + '=';
  $('a').live('click', function (e) {
    var href = $(this).attr('href');
    if (href == null)
      return;
    var pos = href.indexOf(param)
    if (pos < 0)
      return;
    e.preventDefault();
    body.call(this, e, href.slice(pos + param.length));
  });
}

// used for links that do some action (like displaying certain bucket,
// dialog, ...). This function adds support for middle clicking on
// such action links.
function configureActionHashParam(param, body) {
  // this handles normal clicks (NOTE: no change to url/history is
  // done in that case)
  watchHashParamLinks(param, function (e, hash) {
    body(hash);
  });
  // this handles middle clicks. In such case the only hash fragment
  // of our url will be 'param'. We delete that param and call body
  DAO.onReady(function () {
    var value = getHashFragmentParam(param);
    if (value) {
      setHashFragmentParam(param, null);
      body(value, true);
    }
  });
}

var MountPointsStd = mkClass({
  initialize: function (paths) {
    var self = this;
    var infos = _.map(paths, function (p, i) {
      p = self.preprocessPath(p);
      return {p: p, i: i};
    });
    infos.sort(function (a,b) {return b.p.length - a.p.length});
    this.infos = infos;
  },
  preprocessPath: function (p) {
    if (p.charAt(p.length-1) != '/')
      p += '/';
    return p;
  },
  lookup: function (path) {
    path = this.preprocessPath(path);
    var info = _.detect(this.infos, function (info) {
      if (path.substring(0, info.p.length) == info.p)
        return true;
    });
    return info && info.i;
  }
});

var MountPointsWnd = mkClass(MountPointsStd, {
  preprocessPath: (function () {
    var re = /^[A-Z]:\//
    var overriden = MountPointsStd.prototype.preprocessPath;
    return function (p) {
      p = p.replace('\\', '/')
      if (re.exec(p)) { // if we're using uppercase drive letter downcase it
        p = String.fromCharCode(p.charCodeAt(0) + 0x20) + p.slice(1);
      }
      return overriden.call(this, p);
    }
  }())
});

function MountPoints(nodeInfo, paths) {
  if (nodeInfo['os'] == 'windows' || nodeInfo['os'] == 'win32')
    return new MountPointsWnd(paths);
  else
    return new MountPointsStd(paths);
}

function mkTag(name, attrs, contents) {
  if (contents == null)
    contents = '';
  var prefix = ["<", name];
  prefix = prefix.concat(_.map(attrs || {}, function (v,k) {
    return [" ", k, "='", escapeHTML(v), "'"].join('');
  }));
  prefix.push(">");
  prefix = prefix.join('');
  var suffix = ["</",name,">"].join('');
  if (contents instanceof Array)
    contents = _.flatten(contents).join('');
  return prefix + contents + suffix;
}

// proportionaly rescales values so that their sum is equal to given
// number. Output values need to be integers. This particular
// algorithm tries to minimize total rounding error. The basic approach
// is same as in Brasenham line/circle drawing algorithm.
function rescaleForSum(newSum, values, oldSum) {
  if (oldSum == null)
    oldSum = _.inject(values, 0, function (a,v) {return a+v});
  // every value needs to be multiplied by newSum / oldSum
  var error = 0;
  var outputValues = new Array(values.length);
  for (var i = 0; i < outputValues.length; i++) {
    var v = values[i];
    v *= newSum;
    v += error;
    error = v % oldSum;
    outputValues[i] = Math.floor(v / oldSum);
  }
  return outputValues;
}

function extendHTMLAttrs(attrs1, attrs2) {
  if (!attrs2)
    return attrs1;
  for (var k in attrs2) {
    var v = attrs2[k];
    if (k in attrs1) {
      if (k == 'class') {
        v = _.uniq(v.split(/\s+/).concat(attrs1[k].split(/\s+/))).join(' ');
      } else if (k == 'style') {
        v = attrs1[k] + v;
      }
    }
    attrs1[k] = v;
  }
  return attrs1;
}

function usageGaugeHTML(options) {
  var items = options.items;
  var values = _.map(options.items, function (item) {
    return Math.max(item.value, 0);
  });
  var total = _.inject(values, 0, function (a,v) {return a+v});
  values = rescaleForSum(100, values, total);
  var sum = 0;
  // now put cumulative values into array
  for (var i = 0; i < values.length; i++) {
    var v = values[i];
    values[i] += sum;
    sum += v;
  }
  var bars = [];
  for (var i = values.length-1; i >= 0; i--) {
    var style = [
      "width:", values[i], "%;",
      items[i].style
    ].join('');
    bars.push(mkTag("div", extendHTMLAttrs({style: style}, items[i].attrs)));
  }

  var markers = _.map(options.markers || [], function (marker) {
    var percent = calculatePercent(marker.value, total);
    var i;
    if (_.indexOf(values, percent) < 0 && (i = _.indexOf(values, percent+1)) >= 0) {
      // if we're very close to some value, stick to it, so that
      // rounding error is not visible
      if (items[i].value - marker.value < sum*0.01)
        percent++;
    }
    var style="left:" + percent + '%;'
    return mkTag("i", extendHTMLAttrs({style: style}, marker.attrs));
  });

  var tdItems = _.select(options.items, function (item) {
    return item.name != null;
  });

  function formatPair(text) {
    if (text instanceof Array) {
      return [text[0],' (',text[1],')'].join('')
    }
    return text;
  }

  var childs = [
    options.topLeft &&
      mkTag("div",
            extendHTMLAttrs({'class': 'top-left'}, options.topLeftAttrs),
            formatPair(options.topLeft)),
    options.topRight &&
      mkTag("div",
            extendHTMLAttrs({'class': 'top-right'}, options.topRightAttrs),
            formatPair(options.topRight)),
    mkTag("div", extendHTMLAttrs({
      'class': 'usage'
    }, options.usageAttrs), bars.concat(markers)),
    "<table style='width:100%;'><tr>",
    _.map(tdItems, function (item, idx) {
      var extraStyle;
      if (idx == 0)
        extraStyle = 'text-align:left;';
      else if (idx == tdItems.length - 1)
        extraStyle = 'text-align:right;';
      else
        extraStyle = 'text-align:center;';
      return mkTag("td", extendHTMLAttrs({style: extraStyle}, item.tdAttrs),
                   escapeHTML(item.name)
                   + ' ('
                   + escapeHTML(item.renderedValue || item.value) + ')');
    }),
    "</tr></table>"
  ];

  return mkTag("div", options.topAttrs, childs);
}

function memorySizesGaugeHTML(options) {
  var newOptions = _.clone(options);
  newOptions.items = _.clone(newOptions.items);
  for (var i = 0; i < newOptions.items.length; i++) {
    var item = newOptions.items[i];
    if (item.renderedValue)
      continue;
    newOptions.items[i] = item = _.clone(item);
    item.renderedValue = ViewHelpers.formatQuantity(item.value, null, null, ' ');
  }
  return usageGaugeHTML(newOptions);
}

function buildPlotSeries(data, tstamps, breakInterval, timeOffset) {
  var plusInf = -1/0;
  var maxY = plusInf;

  var dataLength = data.length;
  var plotSeries = [];
  var plotData = new Array(dataLength);
  var usedPlotData = 0;
  var prevTStamp;

  plotSeries.push(plotData);

  for (var i = 0; i < dataLength; i++)
    if (data[i] != null)
      break;

  if (i == dataLength)
    return {maxY: 1,
            plotSeries: []};

  var e = data[i];
  if (e >= maxY)
    maxY = e;
  var tstamp = tstamps[i] + timeOffset;
  prevTStamp = tstamp;
  plotData[usedPlotData++] = [tstamp, e];

  for (i++; i < dataLength; i++) {
    e = data[i];
    if (e == null)
      continue;
    if (e >= maxY)
      maxY = e;
    var tstamp = tstamps[i] + timeOffset;
    if (prevTStamp + breakInterval < tstamp) {
      plotData.length = usedPlotData;
      plotData = new Array(dataLength);
      plotSeries.push(plotData);
      usedPlotData = 0;
    }
    prevTStamp = tstamp;
    plotData[usedPlotData++] = [tstamp, e];
  }
  plotData.length = usedPlotData;

  if (maxY == 0 || maxY == plusInf)
    maxY = 1;

  return {maxY: maxY,
          plotSeries: plotSeries};
}


function plotStatGraph(graphJQ, stats, attr, options) {
  options = _.extend({
    color: '#1d88ad',
    verticalMargin: 1.15,
    targetPointsCount: 120
  }, options || {});
  var data = stats[attr] || [];
  var tstamps = stats.timestamp;
  var timeOffset = options.timeOffset || 0;
  var breakInterval = options.breakInterval || 3.1557e+10;

  // not enough data
  if (tstamps.length < 2) {
    tstamps = [];
    data = [];
  }

  var decimation = Math.ceil(data.length / options.targetPointsCount);

  if (decimation > 1) {
    tstamps = decimateNoFilter(decimation, tstamps);
    data = decimateSamples(decimation, data);
  }

  var plotSeries, maxY;
  (function () {
    var rv = buildPlotSeries(data, tstamps, breakInterval, timeOffset);
    plotSeries = rv.plotSeries;
    maxY = rv.maxY;
  })();

  if (options.rate)
    maxY = 100;

  // this is ripped out of jquery.flot which is MIT licensed
  // Tweaks are mine. Bugs too.
  var yTicks = (function () {
    var delta = maxY / 5;

    if (delta == 0.0)
      return [0, 1];

    var size, magn, norm;

    // pretty rounding of base-10 numbers
    var dec = -Math.floor(Math.log(delta) / Math.LN10);

    magn = Math.pow(10, -dec);
    norm = delta / magn; // norm is between 1.0 and 10.0

    if (norm < 1.5)
      size = 1;
    else if (norm < 3) {
      size = 2;
      // special case for 2.5, requires an extra decimal
      if (norm > 2.25) {
        size = 2.5;
      }
    }
    else if (norm < 7.5)
      size = 5;
    else
      size = 10;

    size *= magn;

    var ticks = [];

    // spew out all possible ticks
    var start = 0,
    i = 0, v = Number.NaN, prev;
    do {
      prev = v;
      v = start + i * size;
      ticks.push(v);
      if (v >= maxY || v == prev)
        break;
      ++i;
    } while (true);

    return ticks;
  })();

  if (options.verticalMargin == null)
    var graphMax = maxY;
  else
    var graphMax = yTicks[yTicks.length-1] * options.verticalMargin;

  var preparedQ = ViewHelpers.prepareQuantity(yTicks[yTicks.length-1], 1000);

  function xTickFormatter(val, axis) {
    var unit = axis.tickSize[1];

    var date = new Date(val);

    function fd(value, base) {
      return String(value + base).slice(1);
    }

    function formatWithMinutes() {
      var hours = date.getHours();
      var mins = date.getMinutes();
      var am = (hours > 1 && hours < 13);
      if (!am) {
        if (hours == 0)
          hours = 12;
        else
          hours -= 12;
      }
      if (hours == 12)
        am = !am;
      var formattedHours = fd(hours, 100);
      var formattedMins = fd(mins, 100);

      return formattedHours + ":" + formattedMins + (am ? 'am' : 'pm');
    }

    function formatDate() {
      var monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
      return [monthNames[date.getMonth()], String(fd(date.getDate(), 100))].join(' ')
    }

    var rv;
    switch (unit) {
    case 'minute':
    case 'second':
      rv = formatWithMinutes();
      if (unit == 'second')
        rv = rv.slice(0, -2) + ':' + fd(date.getSeconds(), 100) + rv.slice(-2)
      break;
    case 'hour':
      rv = [formatDate(), formatWithMinutes()].join(' ');
      break;
    case 'day':
      rv = formatDate();
      break;
    default:
      rv = fd(date.getFullYear(), 1000);
    }

    return rv;
  }

  var plotOptions = {
    xaxis: {
      tickFormatter: xTickFormatter,
      mode: 'time',
      ticks: 4
    }, yaxis: {
      tickFormatter: function (val, axis) {
        if (val == 0)
          return '0';
        return [truncateTo3Digits(val/preparedQ[0]), preparedQ[1]].join('');
      },
      min: 0,
      max: graphMax,
      ticks: yTicks
    },
    grid: {
      borderWidth: 0,
      markings: function (opts) {
        // { xmin: , xmax: , ymin: , ymax: , xaxis: , yaxis: , x2axis: , y2axis:  };
        return [
          {xaxis: {from: opts.xmin, to: opts.xmax},
           yaxis: {from: opts.ymin, to: opts.ymin},
           color: 'black'},
          {xaxis: {from: opts.xmin, to: opts.xmin},
           yaxis: {from: opts.ymin, to: opts.ymax},
           color: 'black'}
        ]
      }
    }
  }

  if (options.fixedTimeWidth && tstamps.length) {
    var lastSampleTime = options.lastSampleTime || tstamps[tstamps.length-1];
    plotOptions.xaxis.max = lastSampleTime;
    plotOptions.xaxis.min = lastSampleTime - options.fixedTimeWidth;
  } else if (options.lastSampleTime) {
    plotOptions.xaxis.max = lastSampleTime;
  }

  if (!tstamps.length) {
    plotOptions.xaxis.ticks = [];
  }

  if (options.processPlotOptions) {
    plotOptions = options.processPlotOptions(plotOptions, plotSeries);
  }

  $.plotSafe(graphJQ,
             _.map(plotSeries, function (plotData) {
               return {color: options.color,
                       data: plotData};
             }),
             plotOptions);
}

$.plotSafe = function (placeholder/*, rest...*/) {
  if (placeholder.width() == 0 || placeholder.height() == 0)
    return;
  return $.plot.apply($, arguments);
}
