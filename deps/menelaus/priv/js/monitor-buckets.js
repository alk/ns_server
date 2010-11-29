var MonitorBucketsSection = {
  init: function () {
    var detailedBuckets = BucketsSection.cells.detailedBuckets;

    function filterBucketType(type) {
      return Cell.compute(function (v) {
        var list = _.select(v.need(detailedBuckets), function (bucketInfo) {
          return bucketInfo.bucketType == type;
        });
        var stale = v.need(IOCenter.staleness);
        return {rows: list, stale: stale};
      });
    }

    var membaseBuckets = filterBucketType("membase");
    renderCellTemplate(membaseBuckets, 'monitor_persistent_buckets_list');

    var memcachedBuckets = filterBucketType("memcached");
    renderCellTemplate(memcachedBuckets, 'monitor_cache_buckets_list');

    memcachedBuckets.subscribeValue(function (value) {
      $('#monitor_buckets .memcached-buckets-subsection')[!value || value.rows.length ? 'show' : 'hide']();
    });
    membaseBuckets.subscribeValue(function (value) {
      $('#monitor_buckets .membase-buckets-subsection')[!value || value.rows.length ? 'show' : 'hide']();
    });

    detailedBuckets.subscribeValue(function (list) {
      var empty = list && list.length == 0;
      $('#monitor_buckets .no-buckets-subsection')[empty ? 'show' : 'hide']();
    });

    var stalenessCell = IOCenter.staleness;

    stalenessCell.subscribeValue(function (stale) {
      if (stale === undefined)
        return;
      $('#monitor_buckets .staleness-notice')[stale ? 'show' : 'hide']();
    });
  },
  onEnter: function () {
    BucketsSection.refreshBuckets();
  },
  navClick: function () {
    this.onLeave();
    this.onEnter();
  },
  onLeave: function () {
  }
};
configureActionHashParam('visitStats', $m(AnalyticsSection, 'visitBucket'));
