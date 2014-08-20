angular.module('usageBarDirective', [
]).filter("mnMemSize", function () {
  return function (arg) {
    return _.formatMemSize(arg);
  }
}).directive('mnUsageBar', function () {
  // this is stolen from angular source tree because our current
  // angular is not new enough for scope#$watchGroup
  function ngWatchGroup (watchExpressions, listener) {
    var oldValues = new Array(watchExpressions.length);
    var newValues = new Array(watchExpressions.length);
    var deregisterFns = [];
    var self = this;
    var changeReactionScheduled = false;
    var firstRun = true;

    if (!watchExpressions.length) {
      // No expressions means we call the listener ASAP
      var shouldCall = true;
      self.$evalAsync(function () {
        if (shouldCall) listener(newValues, newValues, self);
      });
      return function deregisterWatchGroup() {
        shouldCall = false;
      };
    }

    if (watchExpressions.length === 1) {
      // Special case size of one
      return this.$watch(watchExpressions[0], function watchGroupAction(value, oldValue, scope) {
        newValues[0] = value;
        oldValues[0] = oldValue;
        listener(newValues, (value === oldValue) ? newValues : oldValues, scope);
      });
    }

    angular.forEach(watchExpressions, function (expr, i) {
      var unwatchFn = self.$watch(expr, function watchGroupSubAction(value, oldValue) {
        newValues[i] = value;
        oldValues[i] = oldValue;
        if (!changeReactionScheduled) {
          changeReactionScheduled = true;
          self.$evalAsync(watchGroupAction);
        }
      });
      deregisterFns.push(unwatchFn);
    });

    function watchGroupAction() {
      changeReactionScheduled = false;

      if (firstRun) {
        firstRun = false;
        listener(newValues, newValues, self);
      } else {
        listener(newValues, oldValues, self);
      }
    }

    return function deregisterWatchGroup() {
      while (deregisterFns.length) {
        deregisterFns.shift()();
      }
    };
  }

  // proportionaly rescales values so that their sum is equal to given
  // number. Output values need to be integers. This particular
  // algorithm tries to minimize total rounding error. The basic approach
  // is same as in Brasenham line/circle drawing algorithm.
  function rescaleForSum(newSum, values, oldSum) {
    if (oldSum == null) {
      oldSum = _.inject(values, function (a,v) {return a+v;}, 0);
    }
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

  function findGroups($element) {
    var lastDt;
    var rv = [];
    _.each($element.children(), function (child) {
      var name = child.nodeName;
      if (name === 'DT') {
        if (lastDt) {
          throw new Error("dt without dd is not supported here");
        }
        lastDt = child;
      } else if (name === 'DD') {
        if (!lastDt) {
          throw new Error("unmatched dd");
        }
        rv.push([lastDt, child]);
        lastDt = null;
      } else {
        console.log("unknown child: ", child);
        throw new Error("Unknown child");
      }
    });
    if (lastDt) {
      throw new Error("dt without dd is not supported here");
    }
    return _.map(rv, function (pair) {
      var dt = pair[0];
      var dd = pair[1];
      if (dd.attributes.length !== 0) {
        throw new Error("dd must be attributeless");
      }
      var ddMatch = dd.innerText.match(/^\{\s*\{(.*)\}\}\s*$/);
      if (!ddMatch) {
        throw new Error("dd contents must be a template");
      }
      var ddTemplate = ddMatch[1];
      var topLeft = false;
      var topRight = false;
      var marker = false;
      var labelColor = '';
      var barColor = '';
      _.each(dt.attributes, function (attr) {
        switch (attr.name) {
        case 'data-top-left':
        case 'top-left':
          topLeft = true;
          return;
        case 'data-top-right':
        case 'top-right':
          topRight = true;
          return;
        case 'data-marker':
        case 'marker':
          marker = true;
          return;
        case 'data-label-color':
        case 'label-color':
          labelColor = String(attr.value);
          return;
        case 'data-bar-color':
        case 'bar-color':
          barColor = String(attr.value);
          return;
        default:
          throw new Error("dt in usage bar only supports specific attrs");
        }
      });

      return {topLeft: topLeft,
              topRight: topRight,
              marker: marker,
              labelColor: labelColor,
              barColor: barColor,
              valueExpr: ddTemplate,
              labelContents: angular.element(dt).contents()};
    });
    return rv;
  }
  return {
    compile: function ($element, $attrs) {
      // this 2 vars are used by link function
      var markers = [];
      var normalGroups = [];

      ;(function () {
        var groups = findGroups($element);
        var topLeftGroup;
        var topRightGroup;

        _.each(groups, function (groupInfo) {
          if (groupInfo.topLeft) {
            topLeftGroup = groupInfo;
            return;
          }
          if (groupInfo.topRight) {
            topRightGroup = groupInfo;
            return;
          }
          if (groupInfo.marker) {
            markers.push(groupInfo);
            return;
          }
          normalGroups.push(groupInfo);
        });

        $element.empty();

        function addLabelContents(element, group) {
          element.append(group.labelContents);
          element.append(" ({{" + group.valueExpr + " | mnMemSize}})")
        }

        if (topLeftGroup) {
          var div = angular.element('<div class="top-left"></div>');
          addLabelContents(div, topLeftGroup);
          $element.append(div);
        }

        if (topRightGroup) {
          var div = angular.element('<div class="top-right"></div>');
          addLabelContents(div, topRightGroup);
          $element.append(div);
        }

        var usageDiv = angular.element('<div class="usage"></div>')
        $element.append(usageDiv);
        var dataUsageClass = $element.attr("data-usage-class");
        if (dataUsageClass) {
          usageDiv.attr("class", dataUsageClass + " usage");
        }

        _.each(normalGroups, function (groupInfo) {
          var barDiv;
          if (groupInfo.barColor) {
            // TODO: make binding color work. Maybe someday.
            barDiv = angular.element('<div style="background-color:' + groupInfo.barColor + ';"></div>');
          } else {
            barDiv = angular.element('<div></div>');
          }
          usageDiv.prepend(barDiv);
        });

        _.each(markers, function (groupInfo) {
          var color = groupInfo.barColor || "#000";
          var markI = angular.element('<i style="background-color:' + color + ';"></i>');
          usageDiv.append(markI);
        });

        var atable = angular.element('<table style="width:100%;"><tbody><tr></tr></tbody></table>');
        var tr = atable.find("tr");
        $element.append(atable);

        var first = true;
        var lastTD;
        _.each(normalGroups, function (groupInfo) {
          // TODO: depend on real jquery which we'll have to add to project
          var td = angular.element(document.createElement("TD"));
          if (groupInfo.labelColor) {
            td.css("color", groupInfo.labelColor);
          }
          addLabelContents(td, groupInfo);
          if (first) {
            td.css("text-align", "left");
          } else {
            td.css("text-align", "center");
          }
          tr.append(td);
          lastTD = td;
          first = false;
        });
        if (lastTD && normalGroups.length > 1) {
          lastTD.css("text-align", "right");
        }
      })();

      return function mnUsageBarLinkFn(scope, actualElement) {
        var usageDiv = angular.element(_.findLast(actualElement.children(), function (e) {return e.nodeName === "DIV";}));
        var normalReversed = _.clone(normalGroups).reverse();
        var hidden = false;

        ;(function () {
          var labelExpressions = _.map(normalReversed, 'valueExpr');
          labelExpressions = labelExpressions.concat(_.map(markers, 'valueExpr'));
          if (scope.$watchGroup) {
            scope.$watchGroup(labelExpressions, onValues);
          } else {
            // old angular
            // TODO: upgrade to newer
            ngWatchGroup.call(scope, labelExpressions, onValues);
          }
        })();

        return;

        function onValues(newValues) {
          if (_.some(newValues, function (v) {return !angular.isDefined(v) || isNaN(v);})) {
            actualElement.css("visibility", "hidden");
            hidden = true;
            return;
          }
          if (hidden) {
            actualElement.css("visibility", "visible");
            hidden = false;
          }
          var barDivs = usageDiv.find("div");
          var total = 0;
          var i;
          // sum only non-marker values
          for (i = normalReversed.length-1; i >= 0; i--) {
            total += newValues[i];
          }
          var normalValues = newValues.slice(0, normalReversed.length);
          var scaledValues = rescaleForSum(100, normalValues, total);
          var runningTotal = 100;
          _.each(normalReversed, function (groupInfo, i) {
            var div = angular.element(barDivs[i]);
            var percent = runningTotal;
            runningTotal -= scaledValues[i];
            div.css("width", "" + percent + "%");
          });
          var markerIs = usageDiv.find("i");
          _.each(markers, function (groupInfo, i) {
            var value = newValues[i + normalReversed.length];
            var percent = (value * 100 / total) | 0;
            var j;
            if (_.indexOf(scaledValues, percent) < 0 && (j = _.indexOf(scaledValues, percent+1)) >= 0) {
              // if we're very close to some value, stick to it, so that
              // rounding error is not visible
              if (normalValues[j] - value < total*0.01) {
                percent++;
              }
            }

            angular.element(markerIs[i]).css("left", "" + percent + "%");
          });
        }
      }
    }
  }
});
