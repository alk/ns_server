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

var documentErrorDef = {
  '502': 'The node containing that document is currently down',
  '503': 'The data has not yet loaded, please wait...',
  '404': 'A document with that ID does not exist',
  'unknown': 'There was an unexpected error'
};

function createDocumentsCells(ns, modeCell, capiBaseCell, bucketsListCell) {
  //bucket
  ns.bucketsListCell = Cell.compute(function (v) {
      var mode = v.need(modeCell);
      if (mode != 'documents') {
        return;
      }
      var buckets = v.need(bucketsListCell).byType.membase;
      var selectedBucketName = v.need(ns.selectedBucketCell);
      return {
        list: _.map(buckets, function (info) { return [info.name, info.name] }),
        selected: selectedBucketName
      };
  }).name("bucketsListCell");

  ns.documentsBucketCell = Cell.compute(function (v) {
    var selected = v(ns.bucketNameCell);
    var buckets = v.need(bucketsListCell).byType.membase;

    if (selected) {
      var bucketNotExist = !_.detect(buckets, function (info) { return info.name === selected });
      if (bucketNotExist) {
        return null;
      } else {
        return selected;
      }
    } else {
      var bucketInfo = _.detect(buckets, function (info) { return info.name === "default" }) || buckets[0];
      if (bucketInfo) {
        return bucketInfo.name;
      } else {
        return null;
      }
    }
  }).name("documentsBucketCell");
  ns.documentsBucketCell.equality = function (a, b) {return a === b;};

  ns.selectedBucketCell = Cell.compute(function (v) {
    var section = v.need(modeCell);
    if (section != 'documents') {
      return;
    }
    return v.need(ns.documentsBucketCell);
  }).name("selectedBucketCell");

  ns.haveBucketsCell = Cell.compute(function (v) {
    return v.need(ns.selectedBucketCell) !== null;
  });

  //document
  ns.currentDocumentIdCell = Cell.compute(function (v) {
    var docId = v(ns.documentIdCell);
    if (docId) {
      return decodeURIComponent(docId);
    }
    return null;
  }).name("currentDocumentIdCell");

  ns.currentDocURLCell = Cell.compute(function (v) {
    var currentDocURL = v.need(ns.currentDocumentIdCell);
    if (!currentDocURL) {
      return;
    }
    return buildDocURL(v.need(ns.dbURLCell), currentDocURL);
  }).name("currentDocURLCell");

  ns.rawCurrentDocCell = Cell.compute(function (v) {
    var dataCallback;

    return future.get({
      url: v.need(ns.currentDocURLCell),
      error: function (xhr) {
        console.log(xhr);
        try {
          JSON.parse(xhr.responseText);
        } catch (e) {
          dataCallback({parserError: true});
          return;
        }
        dataCallback({statusError: xhr.status, });
      },
      success: function (doc) {
        dataCallback(doc || ns.defaultValues.doc);
      }}, undefined, undefined, function (initXHR) {
      return future(function (__dataCallback) {
        dataCallback = __dataCallback;
        initXHR(dataCallback);
      })
    });
  }).name('rawCurrentDocCell');

  ns.currentDocCell = Cell.compute(function (v) {
    if (v.need(ns.haveBucketsCell)) {
      return v.need(ns.rawCurrentDocCell);
    } else {
      return ns.defaultValues.doc;
    }
  }).name("currentDocCell");

  ns.currentDocCell.delegateInvalidationMethods(ns.rawCurrentDocCell);

  //documents list
  ns.currentPageDocsURLCell = Cell.compute(function (v) {
    var param = {};
    param.limit = ns.defaultValues.page_limit.toString();
    param.skip = String(v.need(ns.currentDocumentsPageNumberCell) * ns.defaultValues.page_limit);

    if (ns.searchedDocument) {
      param.startkey = '"' + ns.searchedDocument + '"';
      param.endkey = '"' + ns.searchedDocument + '\u9999"';
    }
    return buildURL(v.need(ns.dbURLCell), "_all_docs", param);
  }).name("currentPageDocsURLCell");

  ns.currentPageDocsCell = Cell.compute(function (v) {
    if (v.need(ns.haveBucketsCell)) {
      return v.need(ns.rawCurrentPageDocsCell);
    } else {
      return {rows: ns.defaultValues.rows, total_rows: ns.defaultValues.total_rows};
    }
  }).name("currentPageDocsCell");

  ns.rawCurrentPageDocsCell = Cell.compute(function (v) {
    return future.get({url: v.need(ns.currentPageDocsURLCell)});
  }).name('rawCurrentPageDocsCell');

  ns.currentPageDocsCell.delegateInvalidationMethods(ns.rawCurrentPageDocsCell);

  ns.dbURLCell = Cell.compute(function (v) {
    var base = v.need(capiBaseCell);
    var bucketName = v.need(ns.selectedBucketCell);

    if (bucketName) {
      return buildURL(base, bucketName) + "/";
    } else {
      return;
    }
  }).name("dbURLCell");

  ns.currentDocumentsPageNumberCell = Cell.compute(function (v) {
    var page = v(ns.documentsPageNumberCell);
    if (page) {
      return Number(page);
    }
    return 0;
  }).name('currentDocumentsPageNumberCell');

  ns.currentPageDocsCellAndCurrentDocCell = Cell.compute(function (v) {
    var currentDocId = v.need(ns.currentDocumentIdCell);
    var source = {};

    if (currentDocId) {
      source.doc = v.need(ns.currentDocCell);
      source.docId = currentDocId;
    } else {
      source.docs = v.need(ns.currentPageDocsCell);
      source.bucketName = v.need(ns.selectedBucketCell);
      source.pageNumber = v.need(ns.currentDocumentsPageNumberCell);
    }
    return source;
  }).name('currentPageDocsCellAndCurrentDocCell');
}

var DocumentsSection = {
  defaultValues: {
    page_limit: 5,
    total_rows: 0,
    rows: [],
    doc: { doesNotExist: true }
  },
  searchedDocument: '',
  init: function () {
    var self = this;

    self.bucketNameCell = new StringHashFragmentCell("bucketName");
    self.documentsPageNumberCell = new StringHashFragmentCell("documentsPageNumber");
    self.documentIdCell = new StringHashFragmentCell("docId");

    createDocumentsCells(self, DAL.cells.mode, DAL.cells.capiBase, DAL.cells.bucketsListCell);
    renderTemplate(documentsListTmpl, {loading: true});

    var documentsListTmpl = 'documents_list';

    var documents = $('#documents');

    var createDocDialog = $('#create_document_dialog');
    var createDocWarning = $('.warning', createDocDialog);
    var createDocInput = $('#new_doc_id', createDocDialog);
    var deleteDocDialog = $("#delete_document_confirmation_dialog");
    var deleteDocDialogWarning = $('.error', deleteDocDialog);

    var breadCrumpDoc = $('#bread_crump_doc', documents);
    var prevNextCont  = $('.ic_prev_next', documents);
    var prevBtn = $('.arr_prev', documents);
    var nextBtn = $('.arr_next', documents);
    var allDocsCont = $('.shadow_box', documents);
    var currenDocCont = $('#documents_details', documents);
    var editingNotice = $('#editing-notice');
    var jsonDocId = $('#json_doc_id', documents);
    var docsLookup = $('#docs_lookup_doc_by_id', documents);
    var docsLookupBtn = $('#docs_lookup_doc_by_id_btn', documents);
    var docDeleteBtn = $('#doc_delete', documents);
    var docSaveAsBtn = $('#doc_saveas', documents);
    var docSaveBtn = $('#doc_save', documents);
    var docCreateBtn = $('.btn_create', documents);
    var docsBucketsSelect = $('#docs_buckets_select', documents);
    var lookupDocForm = $('#search_doc', documents);

    self.jsonCodeEditor = CodeMirror.fromTextArea($("#json_doc")[0], {
      lineNumbers: true,
      matchBrackets: false,
      tabSize: 2,
      mode: { name: "javascript", json: true },
      theme: 'default',
      readOnly: 'nocursor',
      onKeyEvent: function (doc) {
        parseJSON(doc.getValue());
      }
    });

    var documentsDetails = $('#documents_details');
    var codeMirror = $(".CodeMirror", documentsDetails);

    var prevPage;
    var nextPage;
    var pagerEventAllowed;

    _.each([prevBtn, nextBtn, docsLookupBtn, docDeleteBtn, docSaveAsBtn, docSaveBtn, docCreateBtn], function (btn) {
      btn.click(function (e) {
        if ($(this).hasClass('disabled')) {
          e.stopImmediatePropagation();
        }
      })
    });

    function showDocumentListState (source) {
      var total = source.docs.total_rows;
      //currently we can't know is it last page or not for lookup result, example (length == 5 && page_limit == 5)
      var isLastPage = source.docs.rows.length < self.defaultValues.page_limit ? true :
                      (self.defaultValues.page_limit * (source.pageNumber + 1)) >= total;

      prevBtn.toggleClass('disabled', source.pageNumber === 0);
      nextBtn.toggleClass('disabled', isLastPage);

      prevNextCont.show();
      allDocsCont.show();
      currenDocCont.hide();
      hideCodeEditor();

      renderTemplate(documentsListTmpl, {
        loading: false,
        rows: source.docs.rows,
        pageNumber: source.pageNumber,
        bucketName: source.bucketName
      });

      pagerEventAllowed = true;
    }

    function showCodeEditor () {
      self.jsonCodeEditor.setOption('readOnly', false);
      self.jsonCodeEditor.setOption('matchBrackets', true);
      $(self.jsonCodeEditor.getWrapperElement()).removeClass('read_only');
    }

    function hideCodeEditor () {
      self.jsonCodeEditor.setOption('readOnly', 'nocursor');
      self.jsonCodeEditor.setOption('matchBrackets', false);
      $(self.jsonCodeEditor.getWrapperElement()).addClass('read_only');
    }

    function showDocumentState () {
      prevNextCont.hide();
      allDocsCont.hide();
      currenDocCont.show();
      showCodeEditor();
    }

    function validJSONState (source) {
      editingNotice.text('');
      enableSaveBtns();
      jsonDocId.text(source.docId);
      self.jsonCodeEditor.setValue(JSON.stringify(source.doc, null, "  "));
    }

    function invalidJSONState () {
      editingNotice.text('(Editing disabled for non JSON documents)');
      jsonDocId.text('');
      disableSaveBtns();
      hideCodeEditor();
      self.jsonCodeEditor.setValue('');
      docDeleteBtn.removeClass('disabled');
    }

    function disableSaveBtns () {
      docSaveBtn.addClass('disabled');
      docSaveAsBtn.addClass('disabled');
    }

    function enableSaveBtns () {
      docSaveBtn.removeClass('disabled');
      docSaveAsBtn.removeClass('disabled');
    }

    function disableLookup () {
      docsLookup.attr({disabled: true});
      docsLookupBtn.addClass('disabled');
    }

    function enableLookup () {
      docsLookup.attr({disabled: false});
      docsLookupBtn.removeClass('disabled');
    }

    function documentNotExistState () {
      invalidJSONState();
      editingNotice.text('(Document does not exists)');
      docDeleteBtn.addClass('disabled');
    }

    function somethingWrongWithDocument (text) {
      invalidJSONState();
      editingNotice.text('('+ text +')');
      docDeleteBtn.addClass('disabled');
    }

    function documentExistsState (source) {
      docDeleteBtn.removeClass('disabled');
      validJSONState(source);
    }

    function bucketNotExistState () {
      disableLookup();
      docCreateBtn.addClass('disabled');
      prevNextCont.hide();
    }

    function bucketExistsState () {
      enableLookup();
      docCreateBtn.removeClass('disabled');
      prevNextCont.show();
    }

    function parseJSON (json) {
      editingNotice.text('');
      enableSaveBtns();
      try {
        var parsedJSON = JSON.parse(json);
      } catch (error) {
        disableSaveBtns();
        editingNotice.text('(Document is invalid JSON)');
        docDeleteBtn.removeClass('disabled');
        return false;
      }
      return parsedJSON;
    }

    self.selectedBucketCell.subscribeValue(function (selectedBucket) {
      selectedBucket ? bucketExistsState() : bucketNotExistState();
    });

    self.currentDocumentsPageNumberCell.subscribeValue(function (currentPage) {
      prevPage = currentPage - 1;
      prevPage = prevPage < 0 ? 0 : prevPage;
      nextPage = currentPage + 1;

      renderTemplate(documentsListTmpl, {loading: true});
      self.currentPageDocsCellAndCurrentDocCell.recalculate();
    });

    self.currentPageDocsCellAndCurrentDocCell.subscribeValue(function (source) {
      console.log(source)
      if (!source) {
        return;
      }
      if (source.docId) {
          showDocumentState();

        if (source.doc.doesNotExist) {
          documentNotExistState();
          return;
        }
        if (source.doc.parserError) {
          invalidJSONState();
          return;
        }
        if (source.doc.statusError) {
          somethingWrongWithDocument();
        }
        documentExistsState(source);
      } else {
        showDocumentListState(source);
      }
    });

    prevBtn.click(function (e) {
      if (!pagerEventAllowed) {
        return;
      }
      self.documentsPageNumberCell.setValue(prevPage);
      pagerEventAllowed = false;
    });
    nextBtn.click(function (e) {
      if (!pagerEventAllowed) {
        return;
      }
      self.documentsPageNumberCell.setValue(nextPage);
      pagerEventAllowed = false;
    });

    lookupDocForm.submit(function (e) {
      e.preventDefault();
      var docsLookupVal = docsLookup.val().trim();
      if (docsLookupVal) {
        self.documentIdCell.setValue(docsLookupVal);
      }
    });

    breadCrumpDoc.click(function (e) {
      e.preventDefault();
      self.documentIdCell.setValue(undefined);
      self.currentPageDocsCell.recalculate();
    });

    docsBucketsSelect.bindListCell(self.bucketsListCell, {
      onChange: function (e, newValue) {
        self.bucketNameCell.setValue(newValue);
      }
    });

    (function(){
      var latestSearch;
      docsLookup.keyup(function (e) {
        var docsLookupVal = $(this).val().trim();
        if (latestSearch === docsLookupVal) {
          return true;
        }
        latestSearch = docsLookupVal;
        self.searchedDocument = docsLookupVal;
        self.documentsPageNumberCell.setValue(0);
        renderTemplate(documentsListTmpl, { loading: true });
        self.currentPageDocsURLCell.recalculate();
      });
    })();

    //CRUD
    (function () {
      var modal;
      var spinner;
      var dbURL;
      var currentDocUrl;

      self.dbURLCell.subscribeValue(function (url) {;
        dbURL = url;
      });

      self.currentDocURLCell.subscribeValue(function (url) {
        currentDocUrl = url;
      });

      function startSpinner (dialog) {
        modal = new ModalAction();
        spinner = overlayWithSpinner(dialog);
      }

      function stopSpinner () {
        modal.finish();
        spinner.remove();
      }

      function checkOnExistence (newUrl, preDefDoc) {
        couchReq("GET", newUrl, null, 
          function (doc) {
            if (doc) {
              createDocWarning.text("Document with given ID already exists").show();
            }
          },
          function (error) {
            startSpinner(createDocDialog);
            renderTemplate(documentsListTmpl, {loading: true});
            couchReq("PUT", newUrl, preDefDoc, function () {
              stopSpinner();
              hideDialog(createDocDialog);
              self.documentIdCell.setValue(preDefDoc._id);
            }, function (error, num, unexpected) {
              if (error.reason) {
                stopSpinner();
                createDocWarning.text(error.reason)
              } else {
                unexpected();
              }
            });
          }
        );
      }

      docCreateBtn.click(function (e) {
        e.preventDefault();
        createDocWarning.text('');

        showDialog(createDocDialog, {
          eventBindings: [['.save_button', 'click', function (e) {
            e.preventDefault();
            var val = createDocInput.val().trim();
            if (val) {
              var preDefinedDoc = {_id: val};
              var newDocUrl = buildDocURL(dbURL, val);
              checkOnExistence(newDocUrl, preDefinedDoc);
            } else {
              createDocWarning.text("Document ID cannot be empty").show();
            }
          }]]
        });
      });

      docSaveAsBtn.click(function (e) {
        e.preventDefault();
        createDocWarning.text('');

        showDialog(createDocDialog, {
          eventBindings: [['.save_button', 'click', function (e) {
            e.preventDefault();
            var json = parseJSON(self.jsonCodeEditor.getValue());
            if (json) {
              var val = createDocInput.val().trim();
              if (val) {
                var preDefinedDoc = json;
                    preDefinedDoc._id = val;
                var newDocUrl = buildDocURL(dbURL, val);
                checkOnExistence(newDocUrl, preDefinedDoc);
              } else {
                createDocWarning.text("Document ID cannot be empty").show();
              }
            } else {
              hideDialog(createDocDialog);
            }
          }]]
        });
      });

      docSaveBtn.click(function (e) {
        e.preventDefault();
        editingNotice.text('');
        var json = parseJSON(self.jsonCodeEditor.getValue());
        if (json) {
          startSpinner(codeMirror);
          disableSaveBtns();
          docDeleteBtn.addClass('disabled');
          couchReq('PUT', currentDocUrl, json, function (doc) {
            //I think it will be great if we put new document at response
            //self.jsonCodeEditor.setValue(JSON.stringify(doc, null, "\t"));
            stopSpinner();
            docDeleteBtn.removeClass('disabled');
            enableSaveBtns();
          }, function (error, num, unexpected) {
            if (error.reason) {
              stopSpinner();
              editingNotice.text(error.reason)
            } else {
              unexpected();
            }
          });
        }
      });

      docDeleteBtn.click(function (e) {
        e.preventDefault();
        deleteDocDialogWarning.text('');
        showDialog(deleteDocDialog, {
          eventBindings: [['.save_button', 'click', function (e) {
            e.preventDefault();
            startSpinner(deleteDocDialog);
            //deletion of non json document leads to 409 error
            couchReq('DELETE', currentDocUrl, null, function () {
              stopSpinner();
               hideDialog(deleteDocDialog);
              self.documentIdCell.setValue(undefined);
              self.currentPageDocsCell.recalculate();
            }, function (error, num, unexpected) {
              if (error.reason) {
                stopSpinner();
                deleteDocDialogWarning.text(error.reason);
              } else {
                unexpected();
              }
            });
          }]]
        });
      });
    })();
  },
  onLeave: function () {
    self.documentsPageNumberCell.setValue(undefined);
  },
  onEnter: function () {
  },
  navClick: function () {
    this.onLeave();
    this.onEnter();
  }
};
