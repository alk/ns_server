/**
   Copyright 2011 Couchbase, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 **/

// Builds URL for getting/updating given document via CouchDB API
// NOTE: couch does redirect when doing _design%2FXX access, so we
// need special method, instead of using generic buildURL
function buildDocURL(base, docId/*, ..args */) {
  // return Cell with to-be-built URL when any of arguments is a cell
  var args = _.toArray(arguments);
  if ((base instanceof Cell) || (docId instanceof Cell)) {
    return Cell.applyFunctionWithResolvedValues(buildDocURL, this, args);
  }

  if (base.charAt(base.length-1) !== '/') {
    args[0] += '/'; // NOTE: this is base but it's used as part of
                    // args
  }

  if (docId.slice(0, "_design/".length) === "_design/") {
    args.splice(1, 1, "_design", docId.slice("_design/".length));
  } else if (docId.slice(0, "_local/".length) === "_local/") {
    args.splice(1, 1, "_local", docId.slice("_local/".length));
  }
  return buildURL.apply(null, args);
}

function buildViewPseudoLink(bucketName, ddoc, viewName) {
  return encodeURIComponent(_.map([bucketName, ddoc._id, viewName], encodeURIComponent).join("/"));
}

function unbuildViewPseudoLink(link, body, context) {
  if (link.indexOf("/") < 0) {
    link = decodeURIComponent(link);
  }
  body = body || _.toArray;
  return body.apply(context, _.map(link.split("/"), decodeURIComponent));
}

function couchGet(url, callback) {
  IOCenter.performGet({url: url, dataType: "json", success: callback});
}

// now we can pass Cell instead of url and get callback called when
// url cell has value and that url is fetched. Note that passing cells
// that may not ever get defined value is _dangerous_ and will leak.
couchGet = Cell.wrapWithArgsResolving(couchGet);
// same for couchReq, below
couchReq = Cell.wrapWithArgsResolving(couchReq);


function createViewsCells(ns, modeCell, capiBaseCell, bucketsListCell, filterParamsCell) {
  _.each(["modeTabs",
          "subsetTabCell",
          "rawViewsBucketCell",
          "rawDDocIdCell",
          "rawViewNameCell",
          "pageNumberCell",
          "fullSubsetPageNumberCell"], function (name) {
            if (!(name in ns))
              BUG("need defined: " + name);
          });

  ns.viewsBucketCell = Cell.compute(function (v) {
    var selected = v(ns.rawViewsBucketCell);
    if (selected) {
      return selected;
    }
    var buckets = v.need(bucketsListCell).byType.membase;
    var bucketInfo = _.detect(buckets, function (info) {return info.name === "default"}) || buckets[0];
    if (!bucketInfo) {
      return null;
    }
    return bucketInfo.name;
  });
  // NOTE: we're using null as a marker of "known" lack of any
  // value. Thus equality is === (instead of default ==)
  ns.viewsBucketCell.equality = function (a, b) {return a === b;};


  ns.haveBucketsCell = Cell.compute(function (v) {
    return v.need(ns.viewsBucketCell) !== null;
  });

  ns.selectedBucketCell = Cell.compute(function (v) {
    if (v.need(modeCell) != 'views')
      return;
    return v.need(ns.viewsBucketCell);
  }).name("selectedBucket");

  // this contains url to current bucket "database"
  ns.dbURLCell = Cell.compute(function (v) {
    var base = v.need(capiBaseCell);
    var bucketName = v.need(ns.selectedBucketCell);

    if (bucketName) {
      return buildURL(base, bucketName) + "/";
    } else {
      return;
    }
  });

  // this contains url of _all_docs with query params filtering only
  // design docs of current bucket
  ns.allDDocsURLCell = Cell.compute(function (v) {
    return buildURL(v.need(ns.dbURLCell), "_all_docs", {
      startkey: JSON.stringify("_design/"),
      endkey: JSON.stringify("_design0"),
      include_docs: "true"
    });
  }).name("allDDocsURL");

  // this contains result of fetching _all_docs query for design docs of
  // current bucket
  ns.rawAllDDocsCell = Cell.compute(function (v) {
    return future.get({url: v.need(ns.allDDocsURLCell)});
  }).name("rawAllDDocs");

  // this contains list of design docs of current bucket
  ns.allDDocsCell = Cell.compute(function (v) {
    var haveBuckets = v.need(ns.haveBucketsCell);

    if (haveBuckets) {
      return _.map(v.need(ns.rawAllDDocsCell).rows, function (r) {return r.doc});
    } else {
      return [];
    }
  }).name("allDDocs");
  ns.allDDocsCell.delegateInvalidationMethods(ns.rawAllDDocsCell);

  // this contains [] if not inside some particular view and otherwise pair
  // [ddoc, current-view-name]
  // TODO (alk): consider adding Cell suffix
  ns.currentDDocAndView = Cell.computeEager(function (v) {
    var allDDocs = v.need(ns.allDDocsCell);
    var ddocId = v(ns.rawDDocIdCell);
    var viewName = v(ns.rawViewNameCell);
    if (!ddocId || !viewName) {
      return [];
    }
    var ddoc = _.detect(allDDocs, function (d) {return d._id === ddocId});
    if (!ddoc) {
      return [];
    }
    var view = (ddoc.views || {})[viewName];
    if (!view) {
      return [];
    }
    return [ddoc, viewName];
  }).name("currentDDocAndView");

  // this is json of current view if inside view
  ns.currentView = Cell.compute(function (v) {
    return (function (ddoc, viewName) {
      if (viewName === undefined) {
        return;
      }
      return (ddoc.views || {})[viewName];
    }).apply(this, v.need(ns.currentDDocAndView));
  }).name("currentView");

  function mkIntPageCell(pageNumberCell) {
    return Cell.compute(function (v) {
      var pageVal = v(pageNumberCell);
      var pageNo = parseInt(pageVal, 10);
      if (isNaN(pageNo) || pageNo < 1) {
        pageNo = 1;
      }
      if (pageNo > 10) {
        pageNo = 10;
      }
      return pageNo;
    })
  }

  // this contains integer value of dev or productions page number
  // cell (TODO (alk): double check)
  ns.intPageCell = mkIntPageCell(ns.pageNumberCell);
  // this contains integer value of dev "full subset"
  ns.intFullSubsetPageCell = mkIntPageCell(ns.fullSubsetPageNumberCell);

  ns.operatePageNumber = function (body) {
    var cells = [ns.intPageCell, ns.pageNumberCell];
    if (ns.subsetTabCell.value === "prod") {
      cells = [ns.intFullSubsetPageCell, ns.fullSubsetPageNumberCell];
    }
    cells[0].getValue(function (intPage) {
      body(intPage, cells[1]);
    });
  }

  ns.activateNextPage = function () {
    ns.operatePageNumber(function (intPage, cell) {
      if (intPage > 1) {
        cell.setValue((intPage-1).toString());
      }
    });
  }

  ns.activatePrevPage = function () {
    ns.operatePageNumber(function (intPage, cell) {
      if (intPage < 10) {
        cell.setValue((intPage+1).toString());
      }
    });
  }

  // this contains function that accepts page number &
  // production/normal subset and builds query result url NOTE:
  // _function_ depends on pageNo and subset. Things like current
  // bucket, current ddoc, view and filter are parameters of cell. So
  // changing any of them will produce different function.
  //
  // This has subtle (and carefully engineered) effect on back button
  // behavior and on handling of filter & current view changes.
  //
  // The idea is that user needs to specifically run some combination
  // of bucket, ddoc, view and filter and changing any of those need
  // to reset result set and not automatically re-run query.
  //
  // But if only page number is changed it's desired to automatically
  // re-run query and refresh results
  ns.proposedURLBuilderCell = Cell.compute(function (v) {
    if (!v(ns.currentView)) {
      return;
    }
    var dbURL = v.need(ns.dbURLCell);
    var ddocAndView = v.need(ns.currentDDocAndView);
    if (!ddocAndView[1]) {
      return;
    }
    var filterParams = v.need(filterParamsCell);
    return function (pageNo, subset) {
      var initial = {};
      if (subset === "prod") {
        initial.full_set = 'true'
      }
      return buildDocURL(dbURL, ddocAndView[0]._id, "_view", ddocAndView[1], _.extend(initial, filterParams, {
        limit: ViewsSection.PAGE_LIMIT.toString(),
        skip: String((pageNo - 1) * 10)
      }));
    }
  }).name("proposedURLBuilderCell");

  // contains "applied" (i.e. run by user) url builder for default
  // subset
  ns.defaultSubsetAppliedURLBuilderCell = new Cell();
  // contains "applied" (i.e. run by user) url builder for full subset
  ns.fullSubsetAppliedURLBuilderCell = new Cell();
  // contains "applied" (i.e. run by user) url builder for any subset
  ns.lastAppliedURLBuilderCell = new Cell();

  // contains "effective" default subset results url. Will be
  // undefined if current proposedURLBuilderCell is different that
  // last applied default subset builder. proposedURLBuilderCell value
  // being function will always change it's "identity" on any change
  // of it's dependent value. Which means any change of them will
  // reset this to undefined.
  ns.defaultSubsetResultsURLCell = Cell.compute(function (v) {
    var appliedBuilder = v(ns.defaultSubsetAppliedURLBuilderCell);
    var proposedBuilder = v.need(ns.proposedURLBuilderCell);
    if (appliedBuilder !== proposedBuilder) {
      return;
    }
    return appliedBuilder(v.need(intPageCell), "dev");
  });
  // contains "effective" full subset results url. Will be undefined
  // if current proposedURLBuilderCell is different that last applied
  // full subset builder. proposedURLBuilderCell value being function
  // will always change it's "identity" on any change of it's
  // dependent value. Which means any change of them will reset this
  // to undefined.
  ns.fullSubsetResultsURLCell = Cell.compute(function (v) {
    var appliedBuilder = v(ns.fullSubsetAppliedURLBuilderCell);
    var proposedBuilder = v.need(ns.proposedURLBuilderCell);
    if (appliedBuilder !== proposedBuilder) {
      return;
    }
    return appliedBuilder(v.need(ns.intFullSubsetPageCell), "prod");
  });
  // picks either defaultSubsetResultsURLCell or
  // fullSubsetResultsURLCell depending on which subset is choosen.
  ns.viewResultsURLCell = Cell.compute(function (v) {
    if (v.need(ns.subsetTabCell) === "prod") {
      return v.need(ns.fullSubsetResultsURLCell);
    }
    return v.need(ns.defaultSubsetResultsURLCell);
  });

  // "Runs" current combination of bucket, design doc, view and filter
  //
  // Note this is changing value of either
  // fullSubsetAppliedURLBuilderCell or
  // defaultSubsetAppliedURLBuilderCell.
  //
  // The idea is that it should be possible to apply both tabs and see
  // switch between them without re-running query. I found any other
  // behavior I've considered too confusing.
  ns.viewResultsURLCell.runView = function () {
    var urlCell = ns.defaultSubsetResultsURLCell;
    var appliedBuilderCell = ns.defaultSubsetAppliedURLBuilderCell;
    if (ns.subsetTabCell.value === "prod") {
      urlCell = ns.fullSubsetResultsURLCell;
      appliedBuilderCell = ns.fullSubsetAppliedURLBuilderCell;
    }

    // we reset url cell
    urlCell.setValue(undefined);
    Cell.waitQuiescence(function () {
      // then wait until all changes propagate and read proposedURLBuilderCell
      ns.proposedURLBuilderCell.getValue(function (value) {
        // which value we copy to appliedBuilderCell
        appliedBuilderCell.setValue(value);
        ns.lastAppliedURLBuilderCell.setValue(value);
        // and then recalculate urlCell setting it's "true" value
        urlCell.recalculate();
      });
    });
  };

  // this implements the following behavior: if bucket, design doc,
  // view or filter change: ...
  ns.proposedURLBuilderCell.subscribeValue(function (proposed) {
    if (proposed !== ns.lastAppliedURLBuilderCell.value) {
      //  we're resetting subset to default
      ns.subsetTabCell.setValue("dev");
      // reset current page numbers
      ns.pageNumberCell.setValue(undefined);
      ns.fullSubsetPageNumberCell.setValue(undefined);
      // and reset all applied builders which leads all "effective"
      // urls to become undefined
      ns.lastAppliedURLBuilderCell.setValue(undefined);
      ns.defaultSubsetAppliedURLBuilderCell.setValue(undefined);
      ns.fullSubsetAppliedURLBuilderCell.setValue(undefined);
    }
  });

  // this contains results of default subset query (if default subset
  // url is defined)
  ns.defaultSubsetViewResultsCell = Cell.compute(function (v) {
    return future.capiViewGet({url: v.need(ns.defaultSubsetResultsURLCell),
                               timeout: 3600000});
  }).name("defaultSubsetViewResultsCell")

  // this contains results of default subset query (if full subset
  // url is defined)
  ns.fullSubsetViewResultsCell = Cell.compute(function (v) {
    return future.capiViewGet({url: v.need(ns.fullSubsetResultsURLCell),
                               timeout: 3600000});
  }).name("fullSubsetViewResultsCell")

  // this picks between subsets to display
  ns.viewResultsCell = Cell.compute(function (v) {
    // NOTE: we're requiring both subsets values, because otherwise
    // cells would deactivate 'other' results cell as not needed
    // anymore.
    var fullResults = v(ns.fullSubsetViewResultsCell);
    var defaultResults = v(ns.defaultSubsetViewResultsCell);
    return v.need(ns.subsetTabCell) === 'prod' ? fullResults : defaultResults;
  }).name("viewResultsCell");
}

function couchReq(method, url, data, success, error) {
  var postData = {
    type: method,
    url: url,
    dataType: 'json',
    contentType: 'application/json',
    success: success,
    error: function (xhr) {
      var self = this;
      var args = arguments;
      var status = xhr.status;

      function handleUnexpected() {
        return onUnexpectedXHRError.apply(self, args);
      }

      if (!error) {
        return handleUnexpected();
      }

      var parsedJSON = {};

      try {
        parsedJSON = JSON.parse(xhr.responseText);
      } catch (e) {
        // ignore
      }

      return error(parsedJSON, status, handleUnexpected);
    }
  };

  if (data) {
    postData.data = JSON.stringify(data);
  }

  $.ajax(postData);
}

future.capiViewGet = function (ajaxOptions, valueTransformer, newValue, futureWrapper) {
  function missingValueProducer(xhr, options) {
    return {rows: [{key: JSON.parse(xhr.responseText)}]};
  }
  function handleError(xhr, xhrStatus, errMsg) {
    var text;
    try {
      text = xhr.responseText;
    } catch (e) {}
    if (text) {
      try {
        text = JSON.parse(text);
      } catch (e) {
        text = undefined;
      }
    }
    if (!text) {
      text = {error: {xhrStatus: xhrStatus, errMsg: errMsg}};
    }
    dataCallback({rows: [{key: text}]});
  }

  var dataCallback;
  if (ajaxOptions.error || ajaxOptions.missingValueProducer || ajaxOptions.missingValue) {
    BUG();
  }
  ajaxOptions = _.clone(ajaxOptions);
  ajaxOptions.error = handleError;
  ajaxOptions.missingValueProducer = missingValueProducer;
  // we're using future wrapper to get reference to dataCallback
  // so that we can call it from error handler
  return future.get(ajaxOptions, valueTransformer, newValue, function (initXHR) {
    return (future || futureWrapper)(function (__dataCallback) {
      dataCallback = __dataCallback;
      initXHR(dataCallback);
    })
  });
}

function isDevModeDoc(ddoc) {
  var devPrefix = "_design/dev_";
  return ddoc._id.substring(0, devPrefix.length) == devPrefix;
}

var codeMirrorOpts = {
  lineNumbers: true,
    matchBrackets: true,
    mode: "javascript",
    theme: 'default'
};

var ViewsSection = {
  timer: null,
  PAGE_LIMIT: 10,
  mapEditor: CodeMirror.fromTextArea($("#viewcode_map")[0], codeMirrorOpts),
  reduceEditor: CodeMirror.fromTextArea($("#viewcode_reduce")[0], codeMirrorOpts),
  ensureBucketSelected: function (bucketName, body) {
    var self = this;
    ThePage.ensureSection("views");
    Cell.waitQuiescence(function () {
      self.viewsBucketCell.getValue(function (actualBucketName) {
        if (actualBucketName === bucketName) {
          return body();
        }
        self.rawViewsBucketCell.setValue(bucketName);
        ensureBucket(bucketName, body);
      })
    });
  },
  init: function () {
    var self = this;
    var views = $('#views');

    // you can get to local cells through this for debugging
    self.initEval = function (arg) {
      return eval(arg);
    }

    self.modeTabs = new TabsCell("vtab",
                                 "#views .tabs.switcher",
                                 "#views .panes > div",
                                 ["development", "production"]);
    self.modeTabs.subscribeValue(function (tab) {
      views[tab == 'development' ? 'addClass' : 'removeClass']('in-development');
      views[tab == 'development' ? 'removeClass' : 'addClass']('in-production');
    });

    self.subsetTabCell = new LinkClassSwitchCell('dev_subset', {
      firstItemIsDefault: true
    });
    self.subsetTabCell.addItem("subset_dev", "dev");
    self.subsetTabCell.addItem("subset_prod", "prod");
    self.subsetTabCell.finalizeBuilding();

    self.rawViewsBucketCell = new StringHashFragmentCell("viewsBucket");
    self.rawDDocIdCell = new StringHashFragmentCell("viewsDDocId");
    self.rawViewNameCell = new StringHashFragmentCell("viewsViewName");
    self.pageNumberCell = new StringHashFragmentCell("viewPage");
    self.fullSubsetPageNumberCell = new StringHashFragmentCell("fullSubsetViewPage");

    createViewsCells(self,
                     DAL.cells.mode, DAL.cells.capiBase,
                     DAL.cells.bucketsListCell, ViewsFilter.filterParamsCell);

    // this fragment handles current bucket select
    (function () {
      var cell = Cell.compute(function (v) {
        var mode = v.need(DAL.cells.mode);
        if (mode != 'views') {
          return;
        }

        var buckets = v.need(DAL.cells.bucketsListCell).byType.membase;
        var selectedBucketName = v.need(self.viewsBucketCell);
        return {list: _.map(buckets, function (info) {return [info.name, info.name]}),
                selected: selectedBucketName};
      });
      $('#views_bucket_select').bindListCell(cell, {
        onChange: function (e, newValue) {
          self.rawViewsBucketCell.setValue(newValue);
        }
      });
    })();

    // fragment handles disabling of 'Create Development View' button
    // and binds it's click to #startCreateView
    (function (createBtn) {
      self.dbURLCell.subscribeValue(function (value) {
        createBtn.toggleClass('disabled', !value);
      });
      createBtn.bind('click', function (e) {
        e.preventDefault();
        ViewsSection.startCreateView();
      });
    })(views.find('.btn_create'));

    // this fragment renders current ddoc/view select and handles
    // changing of currently selected pair
    (function () {
      var cell = Cell.compute(function (v) {
        var mode = v.need(DAL.cells.mode);
        if (mode != 'views') {
          return;
        }

        var bucketName = v.need(self.viewsBucketCell);

        var ddocs = _.sortBy(v.need(self.allDDocsCell), function(ddoc) {
          return !isDevModeDoc(ddoc);
        });
        var ddocAndView = v.need(self.currentDDocAndView);

        var selectedDDocId = ddocAndView.length && ddocAndView[0]._id;
        var selectedViewName = ddocAndView.length && ddocAndView[1];

        var devOptgroupOutput = false;
        var productionOptgroupOutput = false;
        var groups = _.map(ddocs, function (doc) {
          var rv = "";
          var viewNames = _.keys(doc.views || {}).sort();

          if (isDevModeDoc(doc) && !devOptgroupOutput) {
            rv += '<optgroup label="Development Views" class="topgroup">';
            devOptgroupOutput = true;
          } else if (!isDevModeDoc(doc) && !productionOptgroupOutput) {
            // TODO (alk): that /optgroup is just wrong here
            rv += '</optgroup><optgroup label="Production Views" class="topgroup">';
            productionOptgroupOutput = true;
          }
          rv += "<optgroup label='" + escapeHTML(doc._id) + "' class='childgroup'>";
          _.each(viewNames, function (name) {
            var maybeSelected = (selectedDDocId === doc._id && selectedViewName === name) ? ' selected' : '';
            rv += "<option value='" + escapeHTML(buildViewPseudoLink(bucketName, doc, name)) + "'" +
              maybeSelected + ">" +
              escapeHTML(name) + "</option>";
          });
          rv += "</optgroup>";
          return rv;
        });
        groups.push('</optgroup>');
        return {
          list: groups,
          selected: ddocAndView
        };
      });
      $('#views_view_select').bindListCell(cell, {
        onChange: function (e, newValue) {
          if (!newValue) {
            self.rawDDocIdCell.setValue(undefined);
            self.rawViewNameCell.setValue(undefined);
            return;
          }
          unbuildViewPseudoLink(newValue, function (_ignored, ddocId, viewName) {
            var devMode = isDevModeDoc({_id: ddocId});
            _.defer(function () {
              $('#views_view_select').parent().find('.selectBox-label')
                .html(escapeHTML(ddocId).split('/').join('/<strong>') + '</strong>/_view/<strong>' + escapeHTML(viewName) + '</strong>');
              self.modeTabs.setValue(devMode ? 'development' : 'production');
              self.setCurrentView(ddocId, viewName);
            });
          });
        },
        applyWidget: function () {},
        unapplyWidget: function () {},
        buildOptions: function (q, selected, list) {
          _.each(list, function (group) {
            var option = $(group);
            q.append(option);
          });
          $('#views_view_select').selectBox({autoWidth:false})
            .selectBox('options', list.join(''));
          if (selected.length > 0) {
            $('#views_view_select').parent().find('.selectBox-label')
              .html(escapeHTML(selected[0]._id).split('/').join('/<strong>') + '</strong>/_view/<strong>' + escapeHTML(selected[1]) + '</strong>');
          }
        }
      });
    })();

    // TODO (alk) consider killing currentDDocAndView which looks a
    // bit excessively complex
    DAL.subscribeWhenSection(self.currentDDocAndView, "views", function (value) {
      $('#views_spinner')[value ? 'hide' : 'show']();
      $('#views_list')[(value && !value.length) ? 'show' : 'hide']();
      $('#viewcode')[(value && value.length) ? 'show' : 'hide']();
      $('#view_details')[(value && value.length) ? 'show' : 'hide']();
      $('#view_results_block')[(value && value.length) ? 'show' : 'hide']();
      if (value && value.length) {
        $('#preview_random_doc').trigger('click', true);
        self.mapEditor.refresh();
        self.reduceEditor.refresh();
      }
    });

    function enableEditor(editor) {
      editor.setOption('readOnly', false);
      editor.setOption('lineNumbers', true);
      editor.setOption('matchBrackets', true);
      $(editor.getWrapperElement()).removeClass('read_only');
    }

    function disableEditor(editor) {
      editor.setOption('readOnly', 'nocursor');
      editor.setOption('lineNumbers', false);
      editor.setOption('matchBrackets', false);
      $(editor.getWrapperElement())
        .addClass('read_only')
        .closest('.shadow_box').removeClass('editing');
    }

    // this fragment of code handles displaying current view's map and
    // reduce function bodies to editors and disabling run button of
    // any of those bodies are changed from original value
    (function () {
      var originalMap, originalReduce;

      var justDDocCell = Cell.compute(function (v) {
        return v.need(self.currentDDocAndView)[0];
      });

      Cell.subscribeMultipleValues(function (ddoc, view) {
        $('#views .when-inside-view')[view ? 'show' : 'hide']();
        if (view === undefined) {
          return;
        }
        if (!ddoc) {
          BUG();
        }

        originalMap = view.map;
        originalReduce = view.reduce || "";

        var devView = !!ddoc._id.match(/^_design\/dev_/);
          $('#save_view_as, #just_save_view')[devView ? "show" : "hide"]();

        // NOTE: this triggers onChange right now so it needs to be
        // after setting original{Map,Reduce}
        self.mapEditor.setValue(view.map);
        self.reduceEditor.setValue(view.reduce || "");

        // this may potentially depend on editor being actually shown. So defer a bit
        _.defer(function () {
          if (!devView) {
            disableEditor(self.mapEditor);
            disableEditor(self.reduceEditor);
          } else {
            enableEditor(self.mapEditor);
            enableEditor(self.reduceEditor);
          }
        });
      }, justDDocCell, self.currentView);


      var unchangedCell = new Cell();

      var onMapReduceChange = function () {
        var nowMap = self.mapEditor.getValue();
        var nowReduce = self.reduceEditor.getValue();
        var unchanged = (originalMap === nowMap && originalReduce === nowReduce);
        unchangedCell.setValue(unchanged);
      }

      self.mapEditor.setOption('onChange', onMapReduceChange);
      self.reduceEditor.setOption('onChange', onMapReduceChange);
      unchangedCell.subscribeValue(function (unchanged) {
        $('.run_button').toggleClass('disabled', !unchanged);
      });
    })();

    // this handles rendering of current view results
    self.viewResultsCell.subscribeValue(function (value) {
      if (value) {
        var rows = _.filter(value.rows, function (r) {return ('key' in r)});
        var targ = {rows: rows};
        if (value.errors) {
          var errorsMsg = "Subset of nodes failed with the following error:\n" + JSON.stringify(value.errors, null, "  ");
          postClientErrorReport(errorsMsg);
          genericDialog({text: errorsMsg,
                         buttons: {ok: true, cancel: false}});
        }
      } else {
        var targ = {rows: {lackOfValue: true}};
      }
      renderTemplate('view_results', targ);
    });

    // this code fragment binds next/prev buttons to corresponding
    // actions and handles disabling enabling buttons
    //
    // TODO: move disabling/enabling logic into model
    (function () {
      var prevBtn = $('#views .results_block .arr_prev');
      var nextBtn = $('#views .results_block .arr_next');

      DAL.subscribeWhenSection(Cell.compute(function (v) {
        var intCell = v.need(self.subsetTabCell) === 'prod' ? self.intFullSubsetPageCell : self.intPageCell;
        return [v(self.viewResultsCell), v.need(intCell)];
      }), "views", function (args) {
        if (!args) {
          return;
        }
        (function (viewResults, intPage) {
          if (!viewResults) {
            $('#views .results_block .ic_prev_next').hide();
            return;
          }
          $('#views .results_block .ic_prev_next').show();
          prevBtn.toggleClass('disabled', intPage == 1);
          nextBtn.toggleClass('disabled', (viewResults.rows.length < ViewsSection.PAGE_LIMIT) || intPage == 10);
        }).apply(this, args);
      });

      function withHandlingDisabledness(body) {
        return function (ev) {
          ev.preventDefault();
          if ($(this).hasClass('disabled')) {
            return;
          }
          return body.call(this, ev);
        }
      }

      nextBtn.click(withHandlingDisabledness(self.activateNextPage));
      prevBtn.click(withHandlingDisabledness(self.activatePrevPage));
    })();

    Cell.subscribeMultipleValues(function (url, results) {
      $('#view_results_container')[(url && !results) ? 'hide' : 'show']();
      $('#view_results_spinner')[(url && !results) ? 'show' : 'hide']();
    }, self.viewResultsURLCell, self.viewResultsCell);

    $('#save_view_as').bind('click', $m(self, 'startViewSaveAs'));
    $('#just_save_view').bind('click', $m(self, 'saveView'));

    var productionDDocsCell = Cell.compute(function (v) {
      var allDDocs = v.need(self.allDDocsCell);
      return _.select(allDDocs, function (ddoc) {
        return !isDevModeDoc(ddoc);
      });
    });

    productionDDocsCell.subscribeValue(function(v) {
      if (v !== undefined) {
        if (v.length > 0) {
          $($i('prod_view_count')).html(v.length).parent().fadeIn('fast');
        } else {
          $($i('prod_view_count')).parent().fadeOut('fast');
        }
      }
    });

    var devDDocsCell = Cell.compute(function (v) {
      var allDDocs = v.need(self.allDDocsCell);
      return _.select(allDDocs, function (ddoc) {
        return isDevModeDoc(ddoc);
      });
    });

    // filters tasks of this bucket and converts them from array of
    // tasks to hash from ddoc id to array of tasks of on this ddoc
    // id.
    //
    // This also filters out non-view related tasks
    var tasksOfCurrentBucket = Cell.compute(function (v) {
      var tasks = v.need(DAL.cells.tasksProgressCell);
      var bucketName = v.need(self.selectedBucketCell);
      var rv = {};
      _.each(tasks, function (taskInfo) {
        if (taskInfo.type !== 'indexer' && taskInfo.type !== 'view_compaction') {
          return;
        }
        if (taskInfo.bucket !== bucketName) {
          return;
        }
        var ddoc = taskInfo.designDocument;
        var tasks = rv[ddoc] || (rv[ddoc] = []);
        tasks.push(taskInfo);
      });

      var importance = {
        view_compaction: 1,
        indexer: 2
      };

      _.each(rv, function (tasks, key) {
        // lets also sort tasks in descending importance.
        // NOTE: js's sort is in-place
        tasks.sort(function (taskA, taskB) {
          var importanceA = importance[taskA.type] || BUG("bad type");
          var importanceB = importance[taskB.type] || BUG("bad type");
          return importanceA - importanceB;
        });
      });

      return rv;
    }).name("tasksOfCurrentBucket");
    tasksOfCurrentBucket.equality = _.isEqual;

    function mkViewsListCell(ddocsCell, containerId) {
      var cell = Cell.needing(tasksOfCurrentBucket, ddocsCell, DAL.cells.currentPoolDetailsCell)
        .compute(function (v, tasks, ddocs, poolDetails) {
        var bucketName = v.need(self.selectedBucketCell);
        var rv = _.map(ddocs, function (doc) {
          var rv = _.clone(doc);
          var viewInfos = _.map(rv.views || {}, function (value, key) {
            var plink = buildViewPseudoLink(bucketName, doc, key);
            return _.extend({name: key,
                             viewLink: '#showView=' + plink,
                             removeLink: '#removeView=' + plink
                            }, value);
          });
          viewInfos = _.sortBy(viewInfos, function (info) { return info.name; });
          rv.viewInfos = viewInfos;
          return rv;
        });
        rv.tasks = tasks;
        rv.poolDetails = poolDetails;
        rv.bucketName = bucketName;
        return rv;
      });

      DAL.subscribeWhenSection(cell, "views", function (ddocs) {

        if (!ddocs)
          return;

        var poolDetails = ddocs.poolDetails;
        var bucketName = ddocs.bucketName;

        _.each(ddocs, function (x) {
          var task = (ddocs.tasks[x._id] || [])[0];
          if (task) {
            x.progress = task.progress;
            x.taskType = task.type;
          }
        });

        renderTemplate('views_list', {
          rows: ddocs,
          bucketName: bucketName
        }, $i(containerId));
      });

      return cell;
    }

    var devDDocsViewCell = mkViewsListCell(devDDocsCell, 'development_views_list_container');
    var productionDDocsViewCell = mkViewsListCell(productionDDocsCell, 'production_views_list_container');

    $('#built_in_reducers a').bind('click', function (e) {
      var text = $(this).text();
      var reduceArea = $('#viewcode_reduce');
      if (reduceArea.prop('disabled')) {
        return;
      }
      self.reduceEditor.setValue(text || "");
    });


    $('#view_run_button').bind('click', function (e) {
      if ($(this).hasClass('disabled')) {
        return;
      }
      e.preventDefault();
      self.runCurrentView();
    });

    var currentDocument = null;
    var jsonCodeEditor = CodeMirror.fromTextArea($("#sample_doc_editor")[0], {
      lineNumbers: false,
      matchBrackets: false,
      mode: {name: "javascript", json: true},
      theme: 'default',
      readOnly: 'nocursor'
    });
    $(jsonCodeEditor.getWrapperElement()).addClass('read_only');

    function fetchRandomId(fun) {
      self.dbURLCell.getValue(function (dbURL) {
        couchReq('GET', buildURL(dbURL, '_random'), null, function (data) {
          // TODO: handle failed lookup of cookie in a more friendly way (especially on empty buckets)
          $.cookie("randomKey", data.id);
          fun(data.id);
          $('#sample_docs').show();
          $('#no_sample_docs').hide();
        }, function() {
          $('#sample_docs').hide();
          $('#no_sample_docs').show();
        });
      });
    }

    function randomId(fun) {
      var stored = $.cookie("randomKey");
      if (stored) {
        fun(stored);
      } else {
        fetchRandomId(fun);
      }
    }

    function showDoc(id, error) {
      self.dbURLCell.getValue(function (dbURL) {
        couchReq('GET', buildDocURL(dbURL, id), null, function (data) {
          currentDocument = $.extend({}, data);
          var tmp = JSON.stringify(data, null, "\t");
          $("#edit_preview_doc").removeClass("disabled");
          $("#lookup_doc_by_id").val(data._id);
          jsonCodeEditor.setValue(tmp);
        }, error);
      });
    }


    function docError(alertMsg) {
      var dialog = genericDialog({
        buttons: {ok: true},
        header: "Alert",
        textHTML: alertMsg,
        callback: function (e, btn, dialog) {
          dialog.close();
        }
      });
    }

    $('#preview_random_doc').bind('click', function(ev, dontReset) {

      if (ViewsSection.timer !== null) {
        clearTimeout(ViewsSection.timer);
        ViewsSection.timer = null;
      }

      if (typeof dontReset == "undefined") {
        $.cookie("randomKey", "");
      }
      randomId(function(id) {
        showDoc(id, function(err, status, handleUnexpected) {
          var reason = documentErrorDef[status] || documentErrorDef.unknown;
          $("#edit_preview_doc").addClass("disabled");
          $("#lookup_doc_by_id").val(id);
          jsonCodeEditor.setValue(reason);
          if (status === 503 && ViewsSection.timer === null) {
            ViewsSection.timer = setTimeout(function() {
              $('#preview_random_doc').trigger('click', true);
            }, 2500);
          }
        });
      });
    });


    $('#edit_preview_doc').click(function(ev) {
      ev.stopPropagation();
      if ($(this).hasClass("disabled")) {
        return;
      }
      $('#sample_docs').addClass('editing')
          .find('.darker_block').removeClass('closed');
      enableEditor(jsonCodeEditor);
    });


    $("#lookup_doc_by_id_btn").bind('click', function(e) {
      e.stopPropagation();
      var id = $("#lookup_doc_by_id").val();
      if (id !== "") {
        $.cookie("randomKey", id);
        showDoc(id, function(data, status, handleUnexpected) {
          docError(documentErrorDef[status] || documentErrorDef.unknown);
        });
      } else {
        docError("Id cannot be empty");
      }
    });

    $("#lookup_doc_by_id").bind('click', function(e) {
      e.stopPropagation();
    });

    $("#view_results_container").bind('mousedown', function(e) {
      if ($(e.target).is("a.id")) {
        showDoc($(e.target).text(), function() {
          docError("Unknown Error");
        });
        window.scrollTo(0, 0);
      }
    });

    $('#cancel_preview_doc').click(function(ev) {
      showDoc($.cookie('randomKey'));
      disableEditor(jsonCodeEditor);
    });
    $('#save_preview_doc').click(function(ev) {

      ev.stopPropagation();
      var json, doc = jsonCodeEditor.getValue();
      try {
        json = JSON.parse(doc);
      } catch(err) {
        docError("You entered invalid JSON");
        return;
      }
      json._id = currentDocument._id;
      json._rev = currentDocument._rev;

      for (var key in json) {
        if (key[0] === "$") {
          delete json[key];
        }
      }

      self.dbURLCell.getValue(function (dbURL) {
        couchReq('PUT', buildDocURL(dbURL, json._id), json, function () {
          disableEditor(jsonCodeEditor);
          showDoc(json._id);
        }, function() {
          docError("There was an unknown problem saving your document");
        });
      });
    });

  },
  doDeleteDDoc: function (url, callback) {
    begin();
    function begin() {
      couchGet(url, withDoc);
    }
    function withDoc(ddoc) {
      if (!ddoc) {
        // 404
        callback(ddoc);
        return;
      }
      couchReq('DELETE',
               url + "?" + $.param({rev: ddoc._rev}),
               {},
               function () {
                 callback(ddoc);
               },
               function (error, status, handleUnexpected) {
                 if (status == 409) {
                   return begin();
                 }
                 return handleUnexpected();
               }
              );
    }
  },
  doSaveAs: function (dbURL, ddoc, toId, overwriteConfirmed, callback) {
    var toURL = buildDocURL(dbURL, toId);
    return begin();

    function begin() {
      couchGet(toURL, withDoc);
    }
    function withDoc(toDoc) {
      if (toDoc && !overwriteConfirmed) {
        return callback("conflict");
      }
      ddoc = _.clone(ddoc);
      ddoc._id = toId;
      if (toDoc) {
        ddoc._rev = toDoc._rev;
      } else {
        delete ddoc._rev;
      }
      //TODO: make sure attachments are not screwed
      couchReq('PUT',
               toURL,
               ddoc,
               function () {
                 callback("ok");
               },
               function (error, status, unexpected) {
                 if (status == 409) {
                   if (overwriteConfirmed) {
                     return begin();
                   }
                   return callback("conflict");
                 }
                 return unexpected();
               });
    }
  },
  withBucketAndDDoc: function (bucketName, ddocId, body) {
    var self = this;
    self.ensureBucketSelected(bucketName, function () {
      self.withDDoc(ddocId, body);
    });
  },
  withDDoc: function (id, body) {
    ThePage.ensureSection("views");
    var cell = Cell.compute(function (v) {
      var allDDocs = v.need(ViewsSection.allDDocsCell);
      var ddoc = _.detect(allDDocs, function (d) {return d._id == id});
      if (!ddoc) {
        return [];
      }
      var dbURL = v.need(ViewsSection.dbURLCell);
      return [buildDocURL(dbURL, ddoc._id), ddoc, dbURL];
    });
    cell.getValue(function (args) {
      if (!args[0]) {
        console.log("ddoc with id:", id, " not found");
      }
      args = args || [];
      body.apply(null, args);
    });
  },
  startDDocDelete: function (id) {
    this.withDDoc(id, function (ddocURL, ddoc) {
      if (!ddocURL) // no such doc
        return;

      showDialog('delete_designdoc_confirmation_dialog', {
        eventBindings: [['.save_button', 'click', function (e) {
          e.preventDefault();
          ViewsSection.allDDocsCell.setValue(undefined);
          var spinner = overlayWithSpinner("#delete_designdoc_confirmation_dialog");
          var action = new ModalAction();

          ViewsSection.doDeleteDDoc(ddocURL, function () {
            spinner.remove();
            action.finish();
            ViewsSection.allDDocsCell.recalculate();
            hideDialog('delete_designdoc_confirmation_dialog');
          });
        }]]
      });
    });
  },
  cutOffDesignPrefix: function (id) {
    return id.replace(/^_design\/(dev_|)/, "");
  },
  startDDocCopy: function (id) {
    this.withDDoc(id, function (ddocURL, ddoc, dbURL) {
      if (!ddocURL)
        return;

      var dialog = $('#copy_designdoc_dialog');
      var form = dialog.find("form");

      (function () {
        var name = ViewsSection.cutOffDesignPrefix(ddoc._id);

        setFormValues(form, {
          "ddoc_name": name
        });
      })();

      showDialog(dialog, {
        eventBindings: [['.save_button', 'click', function (e) {
          e.preventDefault();
          var data = $.deparam(serializeForm(form));
          var toId = "_design/dev_" + data.ddoc_name;
          var spinner = overlayWithSpinner($(dialog));
          var needReload = false;

          function closeDialog() {
            spinner.remove();
            hideDialog(dialog);
            if (needReload) {
              ViewsSection.allDDocsCell.recalculate();
              ViewsSection.modeTabs.setValue('development');
            }
          }

          loop(false);

          function loop(overwriteConfirmed) {
            var modal = new ModalAction();
            ViewsSection.doSaveAs(dbURL, ddoc, toId, overwriteConfirmed, function (arg) {
              modal.finish();
              if (arg == "conflict") {
                genericDialog({text: "Please, confirm overwriting target Design Document.",
                               callback: function (e, name, instance) {
                                 instance.close();
                                 if (name != 'ok') {
                                   return closeDialog();
                                 }
                                 loop(true);
                               }});
                return;
              }
              if (arg != 'ok') BUG();
              needReload = true;
              closeDialog();
            });
          }
        }]]
      });
    });
  },
  doSaveView: function (dbURL, ddocId, viewName, overwriteConfirmed, callback, viewDef) {
    var ddocURL = buildDocURL(dbURL, ddocId);
    return begin();
    function begin() {
      couchGet(ddocURL, withDoc);
    }
    function withDoc(ddoc) {
      if (!ddoc) {
        ddoc = {_id: ddocId,
                views: {}};
      }
      var views = ddoc.views || (ddoc.views = {});
      if (views[viewName] && !overwriteConfirmed) {
        return callback("conflict");
      }
      views[viewName] = viewDef || {
        map: "function (doc) {\n  emit(doc._id, null);\n}"
      }
      couchReq('PUT',
               ddocURL,
               ddoc,
               function () {
                 callback("ok");
               },
               function (error, status, unexpected) {
                 if (status == 409) {
                   return begin();
                 }
                 return unexpected();
               });
    }
  },
  startCreateView: function (ddocId) {
    var dbURL = ViewsSection.dbURLCell.value;
    if (!dbURL) {
      return;
    }
    var dialog = $('#copy_view_dialog');
    var warning = dialog.find('.warning').hide();
    dialog.find('[name=designdoc_name], [name=view_name]').val('');
    var ddocNameInput = dialog.find('[name=designdoc_name]').need(1);
    ddocNameInput.prop('disabled', !!ddocId);
    if (ddocId) {
      ddocNameInput.val(this.cutOffDesignPrefix(ddocId));
    }
    showDialog(dialog, {
      title: 'Create Development View',
      closeOnEscape: false,
      eventBindings: [['.save_button', 'click', function (e) {
        e.preventDefault();
        startSaving(ddocNameInput.val(), dialog.find('[name=view_name]').val());
      }]]
    });

    function startSaving(ddocName, viewName) {
      if (!ddocName || !viewName) {
        warning.text("Design Document and View names cannot be empty").show();
        return;
      }
      // TODO: maybe other validation
      var modal = new ModalAction();
      var spinner = overlayWithSpinner(dialog);
      ViewsSection.doSaveView(dbURL, "_design/dev_" + ddocName, viewName, false, function (status) {
        var closeDialog = false;
        if (status == "conflict") {
          warning.text("View with given name already exists").show();
        } else {
          closeDialog = true;
        }
        modal.finish();
        spinner.remove();
        if (closeDialog) {
          ddocNameInput.prop('disabled', false);
          hideDialog(dialog);
          ViewsSection.allDDocsCell.recalculate();
          ViewsSection.modeTabs.setValue("development");
        }
      });
    }
  },
  doRemoveView: function (ddocURL, viewName, callback) {
    return begin();
    function begin() {
      couchGet(ddocURL, withDoc);
    }
    function withDoc(ddoc) {
      if (!ddoc) {
        return;
      }
      var views = ddoc.views || (ddoc.views = {});
      if (!views[viewName]) {
        return;
      }
      delete views[viewName];
      couchReq('PUT',
               ddocURL,
               ddoc,
               function () {
                 callback();
               },
               function (error, status, unexpected) {
                 if (status == 409) {
                   return begin();
                 }
                 return unexpected();
               });
    }
  },
  startRemoveView: function (pseudoLink) {
    return unbuildViewPseudoLink(pseudoLink, this.doStartRemoveView, this);
  },
  doStartRemoveView: function (bucketName, ddocId, viewName) {
    var self = this;
    self.withBucketAndDDoc(bucketName, ddocId, function (ddocURL, ddoc) {
      genericDialog({text: "Deleting this view will result in the index" +
        "for this Design Document to be regenerated when next requested. " +
        "Are you sure you want to delete this view from this Design Document?",
                     callback: function (e, name, instance) {
                       instance.close();
                       if (name === 'ok') {
                         performRemove();
                       }
                     }});

      function performRemove() {
        self.allDDocsCell.setValue(undefined);
        self.doRemoveView(ddocURL, viewName, function () {
          self.allDDocsCell.recalculate();
        });
      }
    });
  },
  startViewSaveAs: function () {
    var self = this;
    var dialog = $('#copy_view_dialog');
    var warning = dialog.find('.warning').hide();

    Cell.compute(function (v) {
      var dbURL = v.need(self.dbURLCell);
      var pair = v.need(self.currentDDocAndView);
      return [dbURL].concat(pair);
    }).getValue(function (args) {
      begin.apply(self, args);
    });

    return;

    function begin(dbURL, ddoc, viewName) {
      if (viewName == null) {
        return;
      }

      var form = dialog.find('form');

      setFormValues(form, {
        'designdoc_name': self.cutOffDesignPrefix(ddoc._id),
        'view_name': viewName
      });

      showDialog(dialog, {
        eventBindings: [['.save_button', 'click', function (e) {
          e.preventDefault();
          var params = $.deparam(serializeForm(form));
          startSaving(dbURL, ddoc, viewName, params['designdoc_name'], params['view_name']);
        }]]
      });
    }

    var spinner;
    var modal;

    function startSaving(dbURL, ddoc, viewName, newDDocName, newViewName) {
      if (!newDDocName || !newViewName) {
        warning.text('Both Design Document name and view name need to be specified').show();
        return;
      }

      spinner = overlayWithSpinner(dialog);
      modal = new ModalAction();

      var newId = "_design/dev_" + newDDocName;

      var mapCode = self.mapEditor.getValue();
      var reduceCode = self.reduceEditor.getValue();

      var view = ddoc.views[viewName];
      if (!view) BUG();
      view = _.clone(view);
      view.map = mapCode;
      if (reduceCode) {
        view.reduce = reduceCode;
      } else {
        delete view.reduce;
      }

      doSaveView(dbURL, newId, newViewName, view, ddoc._id === newId && newViewName === viewName);
    }

    function doSaveView(dbURL, ddocId, viewName, view, overwriteConfirmed) {
      return ViewsSection.doSaveView(dbURL, ddocId, viewName, overwriteConfirmed, callback, view);

      function callback(arg) {
        if (arg === "conflict") {
          return confirmOverwrite(dbURL, ddocId, viewName, view);
        }
        saveSucceeded(ddocId, viewName);
      }
    }

    function confirmOverwrite(dbURL, ddocId, viewName, view) {
      genericDialog({text: "Please, confirm overwriting exiting view.",
                     callback: function (e, name, instance) {
                       if (name == 'ok') {
                         return doSaveView(dbURL, ddocId, viewName, view, true);
                       }
                       modal.finish();
                       spinner.remove();
                     }});
    }

    function saveSucceeded(ddocId, viewName) {
      modal.finish();
      spinner.remove();
      hideDialog(dialog);
      self.rawDDocIdCell.setValue(ddocId);
      self.rawViewNameCell.setValue(viewName);
      self.allDDocsCell.recalculate();
    }
  },
  saveView: function () {
    var self = this;
    var dialog = $('#copy_view_dialog');
    var warning = dialog.find('.warning').hide();

    Cell.compute(function (v) {
      var dbURL = v.need(self.dbURLCell);
      var currentView = v.need(self.currentView);
      var pair = v.need(self.currentDDocAndView);
      return [dbURL, currentView].concat(pair);
    }).getValue(function (args) {
      begin.apply(self, args);
    });

    return;

    function begin(dbURL, currentView, ddoc, viewName) {
      var mapCode = self.mapEditor.getValue();
      var reduceCode = self.reduceEditor.getValue();
      var changed = (currentView.map !== mapCode) || ((currentView.reduce || "") !== reduceCode);

      currentView = _.clone(currentView);
      currentView.map = mapCode;
      if (reduceCode) {
        currentView.reduce = reduceCode;
      } else {
        delete currentView.reduce;
      }
      return ViewsSection.doSaveView(dbURL, ddoc._id, viewName, true, saveCallback, currentView);

      function saveCallback() {
        self.allDDocsCell.recalculate();
      }
    }
  },
  runCurrentView: function () {
    ViewsFilter.closeFilter();
    this.viewResultsURLCell.runView();
  },
  showView: function (plink) {
    return unbuildViewPseudoLink(plink, this.doShowView, this);
  },
  setCurrentView: function (ddocId, viewName) {
    var self = this;
    self.dbURLCell.getValue(function (dbURL) {
      self.rawDDocIdCell.setValue(ddocId);
      self.rawViewNameCell.setValue(viewName);
      ViewsFilter.initFilterFor(buildDocURL(dbURL, ddocId, "_view", viewName));
    });
  },
  doShowView: function (bucketName, ddocId, viewName) {
    var self = this;
    self.withBucketAndDDoc(bucketName, ddocId, function (ddocURL, ddoc) {
      if (!ddoc) {
        return;
      }
      self.setCurrentView(ddoc._id, viewName);
    });
  },
  startPublish: function (id) {
    var self = this;
    self.withDDoc(id, function (ddocURL, ddoc, dbURL) {
      if (!ddocURL) {
        return;
      }
      genericDialog({header: 'Confirm Publishing',
                     text: 'If there is an existing Design Document ' +
                       'in production with this name, it will be overwritten. Please confirm.',
                     buttons: {ok: "Confirm", cancel: true},
                     callback: function (e, name, instance) {
                       if (name != 'ok') {
                         instance.close();
                         return;
                       }
                       publish(instance);
                     }});
      return;

      function publish(dialogInstance) {
        var name = self.cutOffDesignPrefix(ddoc._id);
        var newId = "_design/" + name;
        var modal = new ModalAction();
        var spinner = overlayWithSpinner(dialogInstance.dialog.parent());
        self.doSaveAs(dbURL, ddoc, newId, true, function (arg) {
          if (arg != 'ok') BUG();
          spinner.remove();
          modal.finish();
          dialogInstance.close();
          self.allDDocsCell.recalculate();
          self.modeTabs.setValue('production');
        });
      }
    });
  },
  onEnter: function () {
  },
  onLeave: function () {
    this.rawDDocIdCell.setValue(undefined);
    this.pageNumberCell.setValue(undefined);
    this.rawViewNameCell.setValue(undefined);
    ViewsFilter.reset();
  },
  navClick: function () {
    this.onLeave();
    this.onEnter();
  }
};

configureActionHashParam("deleteDDoc", $m(ViewsSection, "startDDocDelete"));
configureActionHashParam("copyDDoc", $m(ViewsSection, "startDDocCopy"));
configureActionHashParam("showView", $m(ViewsSection, "showView"));
configureActionHashParam("removeView", $m(ViewsSection, "startRemoveView"));
configureActionHashParam("publishDDoc", $m(ViewsSection, "startPublish"));
configureActionHashParam("addView", $m(ViewsSection, "startCreateView"));

var ViewsFilter = {
  rawFilterParamsCell: (function () {
    var klass = mkClass(StringHashFragmentCell, {
      // NOTE: watchHashParamChange is silently converting empty
      // string to undefined we cannot easily fix this as some code
      // might rely on that. Thus we have to request hash parameter
      // and set 'pure' value
      interpretHashFragment: function () {
        this.setValue(getHashFragmentParam(this.paramName));
      }
    });
    return new klass("viewsFilter");
  })(),
  reset: function () {
    if (this.currentViewURL) {
      this.saveFilterIntoHistory();
      this.currentViewURL = null;
    }
    this.rawFilterParamsCell.setValue(undefined);
  },
  init: function () {
    var self = this;

    self.filterParamsCell = Cell.computeEager(function (v) {
      var filterParams = v(self.rawFilterParamsCell);
      if (filterParams == null) {
        if (v.need(ViewsSection.modeTabs) === 'production') {
          filterParams = "stale=update_after&connection_timeout=60000";
        } else {
          filterParams = "connection_timeout=60000";
        }
      }
      filterParams = decodeURIComponent(filterParams);
      return $.deparam(filterParams);
    });

    self.filter = $('#view_results_block .f_popup');

    var selectInstance = self.selectInstance = $('#view_filters');
    selectInstance.selectBox();
    $('#view_filter_stale').selectBox();

    $("#view_results_block .exp_filter").toggle(_.bind(self.openFilter, self),
                                                _.bind(self.closeFilter, self));

    $("#view_filter_add").bind('click', function (e) {
      var value = selectInstance.selectBox("value");
      if (!value) {
        return;
      }
      var input = self.filter.find('[name=' + value + ']');
      if (input.attr('type') === 'checkbox') {
        input.prop('checked', false);
      } else if (value === 'keys') {
        input.val('[""]');
      } else {
        input.val("");
      }
      input.closest('tr').show();
      selectInstance.selectBox("destroy");
      // jquery is broken. this is workaround, because $%#$%
      selectInstance.removeData('selectBoxControl').removeData('selectBoxSettings');
      selectInstance.find("option[value="+value+"]").prop("disabled", true);
      selectInstance.selectBox();
    });

    self.filter.delegate('.btn_x', 'click', function () {
      var row = $(this).closest('tr');
      var name = row.find('[name]').attr('name')
      row.hide();
      selectInstance.selectBox("destroy");
      // jquery is broken. this is workaround, because $%#$%
      selectInstance.removeData('selectBoxControl').removeData('selectBoxSettings');
      selectInstance.find("option[value="+name+"]").prop("disabled", false);
      selectInstance.selectBox();
    });
  },
  openFilter: function () {
    var self = this;
    self.filterParamsCell.getValue(function (params) {
      self.filter.closest('.f_wrap').find('.exp_filter').addClass("selected");
      self.filter.addClass("visib");
      self.fillInputs(params);
    });
  },
  closeFilter: function () {
    if (!this.filter.hasClass('visib')) {
      return;
    }
    this.inputs2filterParams();
    this.filter.closest('.f_wrap').find('.exp_filter').removeClass("selected");
    this.filter.removeClass("visib");
  },
  inputs2filterParams: function () {
    var params = this.parseInputs();
    var packedParams = encodeURIComponent($.param(params));
    var oldFilterParams = this.rawFilterParamsCell.value;
    if (oldFilterParams !== packedParams) {
      this.rawFilterParamsCell.setValue(packedParams);
      ViewsSection.pageNumberCell.setValue(undefined);
      ViewsSection.fullSubsetPageNumberCell.setValue(undefined);
      this.saveFilterIntoHistory();
    }
  },
  iterateInputs: function (body) {
    this.filter.find('.key input, .key select').each(function () {
      var el = $(this);
      var name = el.attr('name');
      var type = (el.attr('type') === 'checkbox') ? 'bool' : 'json';
      var val = (type === 'bool') ? !!el.prop('checked') : el.val();
      body(name, type, val, el);
    });
  },
  fillInputs: function (params) {
    this.iterateInputs(function (name, type, val, el) {
      var row = el.closest('tr');
      if (params[name] === undefined) {
        row.hide();
        return;
      }
      row.show();
      if (type == 'bool') {
        el.prop('checked', !(params[name] === 'false' || params[name] === false));
      } else {
        el.val(params[name]);
      }
    });
    this.selectInstance.selectBox("destroy");
      // jquery is broken. this is workaround, because $%#$%
    this.selectInstance.removeData('selectBoxControl').removeData('selectBoxSettings');
    this.selectInstance.find("option").each(function () {
      var option = $(this);
      var value = option.attr('value');
      option.prop('disabled', value in params);
    });
    this.selectInstance.selectBox();
  },
  parseInputs: function() {
    var rv = {};
    this.iterateInputs(function (name, type, val, el) {
      var row = el.closest('tr');
      if (row.get(0).style.display === 'none') {
        return;
      }
      rv[name] = val;
    });
    return rv;
  },
  loadHistory: function () {
    try {
      return JSON.parse(window.localStorage.filterHistory);
    } catch (e) {
      return [];
    }
  },
  saveHistory: function (history) {
    if (!window.localStorage) {
      return;
    }
    window.localStorage.filterHistory = JSON.stringify(history);
  },
  initFilterFor: function (url) {
    var history = this.loadHistory();
    var entry = _.detect(history, function (entry) {
      return (entry[0] === url);
    });
    this.currentViewURL = url;
    if (!entry) {
      this.rawFilterParamsCell.setValue(undefined);
      return;
    }
    this.rawFilterParamsCell.setValue(entry[1]);
    history = [entry].concat(_.without(history, entry));
    this.saveHistory(history);
  },
  MAX_HISTORY_LENGTH: 256,
  saveFilterIntoHistory: function () {
    var history = this.loadHistory();
    var url = this.currentViewURL;
    if (!url) {
      return;
    }
    var history = _.select(history, function (entry) {
      return entry[0] !== url;
    });
    if (history >= this.MAX_HISTORY_LENGTH) {
      history.pop();
    }
    history.unshift([url, this.rawFilterParamsCell.value]);
    this.saveHistory(history);
  }
};

$(function () {
  ViewsFilter.init();
});
