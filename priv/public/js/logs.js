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

function createLogsSectionCells (ns, modeCell, stalenessCell, tasksProgressCell, logsSectionTabs, serversCell, isROAdminCell) {
  ns.activeTabCell = Cell.needing(modeCell).compute(function (v, mode) {
    return (mode === "log") || undefined;
  }).name('activeTabCell');

  ns.logsRawCell = Cell.needing(ns.activeTabCell).compute(function (v, active) {
    return future.get({url: "/logs"});
  }).name('logsRawCell');

  ns.logsRawCell.keepValueDuringAsync = true;

  ns.massagedLogsCell = Cell.compute(function (v) {
    var logsValue = v(ns.logsRawCell);
    var stale = v.need(stalenessCell);
    if (logsValue === undefined) {
      if (!stale){
        return;
      }
      logsValue = {list: []};
    }
    return _.extend({}, logsValue, {stale: stale});
  }).name('massagedLogsCell');

  ns.isCollectionInfoTabCell = Cell.compute(function (v) {
    return v.need(logsSectionTabs) === "collection_info" &&
           v.need(ns.activeTabCell) && !v.need(isROAdminCell);
  }).name("isClusterTabCell");
  ns.isCollectionInfoTabCell.equality = _.isEqual;

  ns.tasksCollectionInfoCell = Cell.computeEager(function (v) {
    if (!v.need(ns.isCollectionInfoTabCell)) {
      return null;
    }

    var tasks = v.need(tasksProgressCell);
    return _.detect(tasks, function (taskInfo) {
      return taskInfo.type === "collect_logs";
    });
  }).name("tasksRecoveryCell");
  ns.tasksCollectionInfoCell.equality = _.isEqual;

  ns.prepareCollectionInfoNodesCell = Cell.computeEager(function (v) {
    if (!v.need(ns.isCollectionInfoTabCell)) {
      return null;
    }
    var nodes = v.need(serversCell).allNodes;

    return {
      nodes: _(nodes).map(function (node) {
        return {
          nodeClass: node.nodeClass,
          value: node.otpNode,
          isUnhealthy: node.status === 'unhealthy',
          hostname: ViewHelpers.stripPortHTML(node.hostname, nodes)
        };
      })
    }
  }).name("prepareCollectionInfoNodesCell");
  ns.prepareCollectionInfoNodesCell.equality = _.isEqual;
}
var LogsSection = {
  init: function () {
    var collectInfoStartNewView = $("#js_collect_info_start_new_view");
    var selectNodesListCont = $("#js_select_nodes_list_container");
    var uploadToCouchbase = $("#js_upload_to_cb");
    var uploadToForm = $("#js_upload_conf");
    var collectForm = $("#js_collect_info_form");
    var collectFromRadios = $("input[name='from']", collectInfoStartNewView);
    var cancelCollectBtn = $("#js_cancel_collect_info");
    var startNewCollectBtn = $("#js_start_new_info");
    var collectInfoWrapper = $("#js_collect_information");
    var collectResultView = $("#js_collect_result_view");
    var collectResultSectionSpinner = $("#js_collect_info_spinner");
    var showResultViewBtn = $("#js_previous_result_btn");
    var cancelConfiramationDialog = $("#js_cancel_collection_confirmation_dialog");

    var collectInfoViewNameCell = new StringHashFragmentCell("collectInfoViewName");

    var allActiveNodeBoxes;
    var overlay;
    var self = this;

    collectResultSectionSpinner.show();
    collectResultView.hide();
    collectInfoStartNewView.hide();

    self.tabs = new TabsCell('logsTabs', '#js_logs .tabs', '#js_logs .panes > div', ['logs', 'collection_info']);

    createLogsSectionCells(
      self,
      DAL.cells.mode,
      IOCenter.staleness,
      DAL.cells.tasksProgressCell,
      LogsSection.tabs,
      DAL.cells.serversCell,
      DAL.cells.isROAdminCell
    );

    renderCellTemplate(self.massagedLogsCell, 'logs', {
      valueTransformer: function (value) {
        var list = value.list || [];
        return _.clone(list).reverse();
      }
    });

    cancelCollectBtn.click(function (e) {
      if (cancelCollectBtn.hasClass('dynamic_disabled')) {
        return;
      }
      e.preventDefault();

      showDialog(cancelConfiramationDialog, {
        eventBindings: [['.save_button', 'click', function (e) {

          e.preventDefault();
          $.ajax({
            url: '/collectLogs/cancel',
            type: "POST",
            success: function () {
              recalculateTasksUri();
              cancelCollectBtn.addClass('dynamic_disabled');
            },
            error: recalculateTasksUri
          });

          hideDialog(cancelConfiramationDialog);
        }]]
      });

    });
    startNewCollectBtn.click(function (e) {
      e.preventDefault();
      collectInfoViewNameCell.setValue("startNew");
    });
    showResultViewBtn.click(function (e) {
      e.preventDefault();
      collectInfoViewNameCell.setValue("result");
    });
    collectForm.submit(function (e) {
      e.preventDefault();

      $.ajax({
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        type: 'POST',
        url: '/collectLogs/start',
        data: JSON.stringify(getCollectFormValues()),
        success: function (a, b, c) {
          overlay = overlayWithSpinner(collectInfoStartNewView);
          showResultViewBtn.hide();
          self.tasksCollectionInfoCell.changedSlot.subscribeOnce(function () {
            collectInfoViewNameCell.setValue("result");
          });
          recalculateTasksUri();
          hideErrors();
        },
        error: function (resp) {
          hideErrors();
          var errors = JSON.parse(resp.responseText);
          _.each(errors.errors, function (value, key) {
            showErrors(key, value);
          });
        }
      });
    });
    uploadToCouchbase.change(function (e) {
      $('input[type="text"]', uploadToForm).attr('disabled', !$(this).attr('checked'));
    });
    collectFromRadios.change(function (e) {
      if (!allActiveNodeBoxes) {
        return;
      }

      var isAllnodesChecked = $(this).val() == 'allnodes';
      allActiveNodeBoxes.attr('checked', isAllnodesChecked);
      allActiveNodeBoxes.attr('disabled', isAllnodesChecked);
    });

    function removeKeyIfStringIsEmpty(object, key) {
      if (object[key] === "") {
        delete object[key];
      }
    }

    function partial(cb, first) {
      return function (second) {
        cb(first, second);
      }
    }

    function getCollectFormValues() {
      var data = $.deparam(serializeForm(collectForm, undefined, true));
      data.upload = data.upload === "true";
      if (data.from === "nodes") {
        data.nodes = data.nodes ? _.isArray(data.nodes) ? data.nodes : [data.nodes] : [];
      }
      _.each(["customer", "upload_host", "ticket"], partial(removeKeyIfStringIsEmpty, data));

      return data;
    }

    function showErrors(key, value) {
      $("#js_" + key + "_error").text(value).show();
      $("#js_" + key + "_input").addClass("dynamic_input_error");
    }

    function hideErrors() {
      $(".js_error_container", collectInfoStartNewView).hide();
      $(".dynamic_input_error", collectInfoStartNewView).removeClass("dynamic_input_error");
    }

    function recalculateTasksUri() {
      DAL.cells.tasksProgressURI.recalculate();
    }

    function hideResultButtons() {
      startNewCollectBtn.hide();
      cancelCollectBtn.hide();
    }

    function switchCollectionInfoView(isResultView, isCurrentlyRunning, isRunBefore) {
      if (isResultView) {
        collectInfoStartNewView.hide();
        collectResultView.show();
        showResultViewBtn.hide();
        cancelCollectBtn.toggle(isCurrentlyRunning);
        startNewCollectBtn.toggle(!isCurrentlyRunning);
        collectForm[0].reset();
      } else {
        collectInfoStartNewView.show();
        collectResultView.hide();
        hideResultButtons();
        hideErrors();
        showResultViewBtn.toggle(isRunBefore);
      }
    }

    function renderResultView(collectionInfo) {
      var templateData = {
        nodesByStatus: _.groupBy(collectionInfo.perNode, 'result'),
        isCompleted: collectionInfo.status === "idle",
        details: _.filter(collectionInfo.perNode, function (node) {
          return node.details && node.details.length;
        })
      };

      renderTemplate('js_collect_progress', templateData);
    }

    function renderStartNewView() {
      self.prepareCollectionInfoNodesCell.getValue(function (selectNodesList) {
        renderTemplate('js_select_nodes_list', selectNodesList);
        allActiveNodeBoxes = $('input:not(:disabled)', selectNodesListCont);
        collectFromRadios.eq(0).attr('checked', true).trigger('change');
        uploadToCouchbase.attr('checked', true).trigger('change');
      });
    }

    self.isCollectionInfoTabCell.subscribeValue(function (isCollectionInfoTab) {
      if (!isCollectionInfoTab) {
        hideResultButtons();
        showResultViewBtn.hide();
      }
    });

    Cell.subscribeMultipleValues(function (collectionInfo, tabName) {
      if (!collectionInfo) {
        return;
      }
      var isCurrentlyRunning = collectionInfo.status === 'running';
      var isRunBefore = !!collectionInfo.perNode.length;
      var isResultView = tabName === 'result';

      !isCurrentlyRunning && cancelCollectBtn.removeClass('dynamic_disabled');

      if (isCurrentlyRunning) {
        collectInfoViewNameCell.setValue("result");
      } else {
        if (tabName) {
          if (!isRunBefore) {
            collectInfoViewNameCell.setValue("startNew");
          }
        } else {
          var defaultTabName = isRunBefore ? "result": "startNew";
          collectInfoViewNameCell.setValue(defaultTabName);
        }
      }

      switchCollectionInfoView(isResultView, isCurrentlyRunning, isRunBefore);
      collectResultSectionSpinner.hide();

      if (overlay) {
        overlay.remove();
      }
      if (isResultView) {
        renderResultView(collectionInfo);
      } else {
        renderStartNewView();
      }
    }, self.tasksCollectionInfoCell, collectInfoViewNameCell);

    self.massagedLogsCell.subscribeValue(function (massagedLogs) {
      if (massagedLogs === undefined){
        return;
      }
      var stale = massagedLogs.stale;
      $('#js_logs .staleness-notice')[stale ? 'show' : 'hide']();
    });

    self.logsRawCell.subscribe(function (cell) {
      cell.recalculateAfterDelay(30000);
    });
  },
  onEnter: function () {
  },
  navClick: function () {
    if (DAL.cells.mode.value == 'log'){
      this.logsRawCell.recalculate();
    }
  },
  domId: function (sec) {
    return 'logs';
  }
}