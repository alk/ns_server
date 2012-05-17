#!/usr/bin/ruby

require 'rubygems'
require 'json'
require 'pp'
require 'active_support/all'
require 'set'

module Event
  class Base
    def initialize(hash)
      @hash = hash
      bad_keys = []
      @hash.each do |k,v|
        if k.to_s.underscore != k.to_s
          bad_keys << k
        end
      end
      bad_keys.each do |k|
        v = @hash[k]
        @hash.delete k
        @hash[k.to_s.underscore.to_sym] = v
      end
      raise unless Event.type_to_class_name(@hash[:type]) != self.class.name
    end
    def __event_respond_to?(sym)
      @hash.has_key? sym
    end
    def method_missing(sym, *args)
      super unless __event_respond_to?(sym) && args.size == 0
      @hash[sym]
    end
    def respond_to?(sym)
      __event_respond_to?(sym) || super
    end

    def self.ping!; :pong; end

    def raw_hash; @hash; end

    def raw_ts
      @hash[:ts]
    end

    def ts
      @ts ||= Time.at(@hash[:ts])
    end
  end

  mattr_accessor :known_event_types
  self.known_event_types = %w[rebalanceStart updateMap updateFastForwardMap rebalanceEnd
                              deregisterTapName ebucketmigratorStart ebucketmigratorTerminate
                              vbucketMoverStart vbucketMoveDone resetFastForwardMap notReadyVBuckets]

  def self.type_to_class_name(type)
    type.underscore.camelize
  end

  def self.maybe_from_hash(hash)
    hash.symbolize_keys!
    type = hash[:type]
    return unless self.known_event_types.include? type
    self.const_get(Event.type_to_class_name(type).intern).new(hash)
  end

  def self.from_hash(hash)
    self.maybe_from_hash(hash) || raise("unknown type #{hash[:type]}")
  end

  self.known_event_types.each do |event_type|
    module_eval "class #{Event.type_to_class_name event_type} < Event::Base; end"
  end

end

module BeforeAfterChains
  # NOTE: some offline events formatting sometimes causes nodes to be
  # identified by their erlang node names instead of memcached
  # host:port strings. So lets "normalize" former into later
  def raw_chain_before
    @raw_chain_before ||= @hash[:chain_before] || @hash[:old_chain]
  end
  def raw_chain_after
    @raw_chain_after ||= @hash[:chain_after] || @hash[:new_chain]
  end

  def self.normalize_hostname(name)
    return name unless name =~ /\Ans_1@/
    $' + ":11209"
  end

  NORMALIZE_HOSTNAME = self.method(:normalize_hostname)

  def chain_before
    @chain_before ||= self.raw_chain_before.map(&NORMALIZE_HOSTNAME)
  end
  def chain_after
    @chain_after ||= self.raw_chain_after.map(&NORMALIZE_HOSTNAME)
  end
end

Event::RebalanceStart.ping!
Event::UpdateMap.ping!
Event::UpdateMap.send :include, BeforeAfterChains
Event::UpdateFastForwardMap.ping!
Event::UpdateFastForwardMap.send :include, BeforeAfterChains
Event::RebalanceEnd.ping!
Event::DeregisterTapName.ping!
class Event::DeregisterTapName
  def full_name
    @full_name ||= [self.bucket, self.host, self.name]
  end
end
Event::EbucketmigratorStart.ping!
class Event::EbucketmigratorStart
  def raw_bucket
    @hash[:bucket]
  end
  def bucket
    @hash[:username]
  end
  def full_name
    @full_name ||= [self.bucket, self.src, self.name]
  end
end
Event::EbucketmigratorTerminate.ping!
Event::VbucketMoverStart.ping!
Event::VbucketMoverStart.send :include, BeforeAfterChains
Event::VbucketMoveDone.ping!
Event::ResetFastForwardMap.ping!
Event::NotReadyVBuckets.ping!

class Object
  def self.class_init_attrs
    @init_attrs ||= {}
  end

  def self.local_init_args_order
    @local_init_args_order ||= []
  end

  def self.read_init_args_order
    rv = @local_init_args_order
    rv || if (s = self.superclass)
            s.read_init_args_order
          else
            []
          end
  end

  def self.all_init_attrs(update_this = {})
    if (s = self.superclass)
      s.all_init_attrs(update_this)
    end
    update_this.update(self.class_init_attrs)
    update_this
  end

  def self.init_attr(*attrs)
    attrs.each {|a| self.class_init_attrs[a] = true}
    self.local_init_args_order.concat(attrs)
    attr_reader(*attrs)
  end

  def init_attrs!(args, order, klass = self.class)
    # puts "order: #{order.pretty_inspect}"
    # puts "args: #{args.pretty_inspect}"
    if args.size < order.size
      raise "too short args list (#{args.size} versus #{order.size})"
    end
    understood_args = klass.all_init_attrs
    # puts "understood_args: #{understood_args.pretty_inspect}"
    prefix = if args.size - 1 == order.size && args[-1].kind_of?(Hash)
               args[-1].to_a
             elsif args.size == order.size
               []
             else
               raise "too long args list (#{args.size} versus #{order.size})"
             end
    (prefix + order.zip(args[0...(order.size)])).each do |(var, value)|
      raise "unknown arg #{var}" unless understood_args.has_key? var.to_sym
      self.instance_variable_set('@'+var.to_s, value)
    end
    @__args_inited = true
  end

  def self.just_init_args!(order = nil, &block)
    raise unless order == nil || order.kind_of?(Array)
    self.send(:define_method, :initialize) do |*args|
      unless @__args_inited
        order ||= self.class.read_init_args_order
        self.init_attrs!(args, order)
      end
      super unless block
      self.instance_eval(&block) if block
    end
  end
end

module TestInitAttrsThingy
  class Base
    attr_reader :inited
    init_attr :a, :b
    just_init_args! do
      @inited = true
    end
    def self.verify
      instance = self.new(:av, :bv)
      raise unless instance.a == :av
      raise unless instance.b == :bv
      raise unless instance.inited
    end
  end

  class BaseAlt
    attr_reader :inited
    init_attr :a, :b
    just_init_args! [:a] do
      @inited = true
    end
    def self.verify
      instance = self.new(:av, :b => :bv)
      raise unless instance.a == :av
      raise unless instance.b == :bv
      raise unless instance.inited

      instance = self.new(:av)
      raise unless instance.a == :av
      raise unless instance.b == nil
      raise unless instance.inited
    end
  end

  class Derived1 < Base
    init_attr :c
    just_init_args! [:a, :b, :c]

    def self.verify
      instance = self.new(:av, :bv, :cv)
      raise unless instance.a == :av
      raise unless instance.b == :bv
      raise unless instance.c == :cv
      raise unless instance.inited
    end
  end

  class Derived3 < Base
  end

  class Derived2 < Base
    just_init_args! [:a, :b] do
    end

    def self.verify
      instance = self.new(:av, :bv)
      raise unless instance.a == :av
      raise unless instance.b == :bv
      raise unless !instance.inited
    end
  end

  def self.verify!
    [Base, BaseAlt, Derived1, Derived2, Derived3].each(&:verify)
  end

  self.verify!
end

module TestMismatch
  def assert_equals!(expected, actual, extra = "(no details)", inspect_method = :pretty_inspect)
    return if expected == actual
    if block_given?
      puts "got assert failing details:"
      yield
    end
    raise "#{extra}. Expected #{expected.send inspect_method} got #{actual.send inspect_method}"
  end
end

module IdentifyTasks
  include Event
  include TestMismatch
  extend TestMismatch

  Event::Base.ping!
  module EventExtension
    attr_accessor :task

    def assert_after! other_event
      return if other_event.raw_ts < self.raw_ts
      raise "after assertion failed for\n#{other_event.pretty_inspect}\n#{self.pretty_inspect}"
    end
  end
  Event::Base.send :include, EventExtension

  class VBucketReplicationStart
    include EventExtension

    attr_reader :vbucket
    attr_reader :migrator_start
    attr_reader :was_not_ready
    delegate :ts, :raw_ts, :bucket, :name, :full_name, :src, :dst, :to => :migrator_start
    def initialize(vbucket, migrator_start, was_not_ready)
      @vbucket, @migrator_start, @was_not_ready = vbucket, migrator_start, was_not_ready
    end

    def raw_hash
      {
        :type => "VBucketReplicationStart",
        :vbucket => @vbucket,
        :migrator_start => @migrator_start.raw_hash,
        :was_not_ready => @was_not_ready
      }
    end
  end

  class VBucketReplicationStop
    include EventExtension
    # extend ActiveSupport::Memoizable

    attr_reader :vbucket
    attr_reader :migrator_terminate

    delegate :ts, :raw_ts, :to => :migrator_terminate
    delegate :bucket, :name, :full_name, :src, :dst, :to => :migrator_start
    # memoize :bucket, :name, :full_name, :src, :dst

    def initialize(vbucket, migrator_terminate)
      @vbucket, @migrator_terminate = vbucket, migrator_terminate
    end
    def migrator_start
      @migrator_start ||= @migrator_terminate.task.start_event
    end

    def raw_hash
      {
        :type => "VBucketReplicationStop",
        :vbucket => @vbucket,
        :migrator_terminate => @migrator_terminate.raw_hash,
        :migrator_start => self.migrator_start.raw_hash
      }
    end
  end

  class BaseTask
    include TestMismatch
    extend TestMismatch
    include Event

    attr_reader :events, :subtasks, :parent_task
    def initialize
      @events = []
      @subtasks = []
      @parent_task = nil
      @structure_frozen = false
    end
    alias :initialize_base_task! :initialize

    def freeze_structure!
      return if @structure_frozen
      @subtasks.each(&:freeze_structure!)
      self.sort_events!
      @events.freeze
      @subtasks.freeze
      @structure_frozen = true
      # freeze
    end

    def sort_events!
      @subtasks = @subtasks.sort_by {|st| st.first_event.raw_ts}
      @events = @events.sort_by(&:raw_ts)
    end

    def first_event
      @first_event ||=
        begin
          raise unless @structure_frozen
          [self.events[0], (first_subtask = self.subtasks[0]) && first_subtask.first_event].compact.min_by(&:raw_ts)
        end
    end
    def last_event
      @last_event ||=
        begin
          raise unless @structure_frozen
          [self.events[-1], (last_subtask = self.subtasks[-1]) && last_subtask.last_event].compact.max_by(&:raw_ts)
        end
    end

    def all_events
      @all_events ||=
        begin
          raise unless @structure_frozen
          self.subtasks.inject(self.events.dup) {|acc, st| acc.concat(st.all_events)}.sort_by(&:raw_ts)
        end
    end

    def claim_event(event)
      return self if event.task == self
      if event.task
        raise "event #{event.pretty_inspect} already claimed"
      end
      event.task = self
      @events << event
      self
    end

    def claim_slot(slot, value)
      raise "trying to claim nil for #{slot}" unless value
      assert_equals! nil, self.send(slot), "claiming slot: #{slot}"
      instance_variable_set("@"+slot.to_s, value)
    end

    def claim_event_slot(slot, event)
      claim_event(event)
      claim_slot(slot, event)
    end

    def parent_task!(parent)
      raise if @parent_task
      @parent_task = parent
      @parent_task.subtasks << self
      self
    end

    def self.claimable_slot(*slots)
      attr_reader(*slots)
      slots.each do |slot|
        class_eval "def claim_#{slot}(value); claim_slot :#{slot}, value; end"
      end
    end

    def self.event_slot(*slots)
      attr_reader(*slots)
      slots.each do |slot|
        class_eval "def claim_#{slot}(ev); claim_event_slot :#{slot}, ev; end"
      end
    end
  end

  class RebalanceTask < BaseTask
    init_attr :start_event, :end_event
    attr_reader :bucket_subrebalances
    just_init_args! do
      # super doesn't work eh
      initialize_base_task!
      @bucket_subrebalances = {}
    end

    def bucket_subtask(bucket)
      @bucket_subrebalances[bucket] ||= BucketRebalanceTask.new(bucket).parent_task!(self)
    end
  end

  class BucketRebalanceTask < BaseTask
    init_attr :bucket
    attr_reader :target_map
    attr_reader :previous_map
    attr_reader :all_vbucket_subtasks
    event_slot :final_fast_forward_reset
    just_init_args! do
      initialize_base_task!
      @target_map = []
      @previous_map = []
      @all_vbucket_subtasks = {}
    end

    def claim_ff_chain(ff_map_event)
      self.claim_event(ff_map_event)
      vbucket = ff_map_event.vbucket
      assert_equals! nil, @previous_map[vbucket], "(target_map chain not claimed yet)"
      @target_map[vbucket] = ff_map_event.chain_after
    end

    def self.perform_claim_ff_update_pass(master_task, events)
      events.each do |ev|
        case ev
        when UpdateFastForwardMap
          master_task.bucket_subtask(ev.bucket).claim_ff_chain(ev)
        when ResetFastForwardMap
          master_task.bucket_subtask(ev.bucket).claim_final_fast_forward_reset(ev)
        end
      end
    end

    # assumes ff pass updated @target_map
    def process_map_update(map_event)
      vbucket = map_event.vbucket
      assert_equals! nil, @previous_map[vbucket], "(previous_map chain not claimed yet)"
      @previous_map[vbucket] = map_event.chain_before
      assert_equals! @target_map[vbucket], map_event.chain_after, "after versus target chain mismatch"
    end

    def self.perform_map_update_pass(master_task, events)
      events.each do |ev|
        case ev
        when UpdateMap
          bucket_subtask = master_task.bucket_subtask(ev.bucket)
          bucket_subtask.process_map_update(ev)
          bucket_subtask.vbucket_subtask(ev.vbucket).claim_map_update_event(ev)
        end
      end
    end

    def vbucket_subtask(vbucket)
      @all_vbucket_subtasks[vbucket] ||= VBucketMovementTask.new(vbucket).parent_task!(self)
    end
  end

  class VBucketMovementTask < BaseTask
    init_attr :vbucket
    event_slot :start_event, :end_event
    event_slot :map_update_event
    attr_reader :takeover_subtask
    attr_reader :replica_building_tasks

    just_init_args! do
      initialize_base_task!
      @replica_building_tasks = []
    end

    def claim_map_update_event(ev)
      claim_event_slot :map_update_event, ev
      assert_equals! self.parent_task.target_map[ev.vbucket], ev.chain_after, "chain_after"
      assert_equals! self.parent_task.previous_map[ev.vbucket], ev.chain_before, "chain_before"
    end

    def claim_start_event(ev)
      claim_event_slot :start_event, ev
      # NOTE: we're assuming map_update_event is already claimed
      assert_equals! self.map_update_event.chain_before, ev.chain_before
      assert_equals! self.map_update_event.chain_after, ev.chain_after
    end

    def claim_takeover_subtask(subtask)
      subtask.parent_task! self
      claim_slot(:takeover_subtask, subtask)
    end

    def note_replica_building_task(subtask)
      subtask.parent_task! self
      self.replica_building_tasks << subtask
    end

    class ReplicationPairState
      attr_reader :src, :dst, :prev
      def initialize(src, dst, prev = nil)
        @src, @dst = src, dst
        @prev = prev
      end

      def associate_event(ev_hash, requested_transition)
        if @event_hash
          # FIXME: that's not clean OO but lets make it work first
          raise unless requested_transition == :on_during_take_over
          return
        end
        @event_hash = ev_hash
      end

      def name; self.class.name; end

      def transition(klass)
        klass.new(@src, @dst, self)
      end
      private :transition

      def on_illegal_transition
        throw :illegal_transition
        # raise "illegal event #{method} in state replication state #{self.name} between #{self.src} and #{self.dst}"
      end

      # on_during_take_over happens between all other pairs to
      # validate if current state is valid during takeover
      %w[on_replication_stop on_replication_start on_star_start on_star_stop
         on_take_over_start on_take_over_done on_during_take_over].each do |state_method|
        alias_method state_method.to_sym, :on_illegal_transition
      end

      def self.initial(src, dst)
        None.new(src, dst)
      end

      def invalid_final_state?(pre_chain, post_chain)
        pair = [@src,@dst]
        # TODO: consider replication legs that are never stopped or
        # started
        case self
        when ReplicationStoppedTotally
          # if we stopped replication in the end it must be something
          # in old chain that's not needed anymore
          src_idx = pre_chain.find_index(@src)
          return :src_idx unless src_idx
          dst_idx = pre_chain.find_index(@dst)
          return :dst_idx unless dst_idx
          return :idx_pos unless dst_idx == src_idx + 1
          post_chain.find_index(@dst) ? :post_chain : false
        when TakeOverDone
          # if that was takeover we check it was between old & new master
          return :post_chain unless post_chain[0] == @dst
          return :pre_chain unless pre_chain[0] == @src

          prev_state = self.prev
          raise unless prev_state.kind_of?(TakeOverStarted)

          prev_state = prev_state.prev
          raise unless prev_state.kind_of?(StarStopped)

          prev_state = prev_state.prev
          raise unless prev_state.kind_of?(StarStarted)

          # and that pre-star state was either replication stopped (if
          # new master was replica) or none (if it's brand-new for
          # this vbucket)
          prev_state = prev_state.prev
          expected_kind = pre_chain.include?(@dst) ? ReplicationStopped : None
          prev_state.kind_of?(expected_kind) ? false : :pre_star
        when ReplicationStarted
          # replication started pairs must exist in new post_chain
          src_idx = post_chain.find_index(@src)
          dst_idx = post_chain.find_index(@dst)
          return :post_chain unless src_idx && dst_idx && dst_idx == src_idx + 1
          false
        when ReplicationStartedFromNone, ReplicationActuallyStartedAfterTakeover
          return :must_not_be_in_pre_chain if pre_chain.each_cons(2).include? pair
          if self.kind_of?(ReplicationStartedFromNone)
            return :no_takeover_expected if pre_chain[0] && pre_chain[0] != post_chain[0]
          end
          return :post_chain unless post_chain.each_cons(2).include? pair
          false
        when StarStopped
          # star stopped is valid for pairs that are only used during
          # replica building
          return :star_stopped_from_bad_src unless pre_chain[0] == @src
          return :replication_missing if post_chain.each_cons(2).include? pair
          false
        else
          :bad_state
        end
      end

      def assert_valid_end_state!(pre_chain, post_chain)
        invalid = self.invalid_final_state?(pre_chain, post_chain)
        if invalid
          raise "bad end state for #{self.pretty_inspect}"+
            " because of #{invalid.inspect} (#{pre_chain.pretty_inspect} -> #{post_chain.pretty_inspect})"
        end
      end

      # something will happen between this pair but in future
      class None < ReplicationPairState
        def on_replication_stop
          transition ReplicationStopped
        end

        def on_star_start
          transition StarStarted
        end

        def on_during_take_over
          transition ReplicationLeftRunning
        end

        def on_replication_start
          # we expect this to happen if no "on_during_take_over"
          # happened and replication is part of new chain but not part of old chain
          transition ReplicationStartedFromNone
        end
      end

      # this replication was found to be "on" during takeover
      class ReplicationLeftRunning < ReplicationPairState
        def on_replication_stop
          transition ReplicationStoppedTotally
        end

        def on_replication_start
          transition ReplicationActuallyStartedAfterTakeover
        end

        def on_during_take_over
          self
        end
      end

      class ReplicationActuallyStartedAfterTakeover < ReplicationPairState
      end

      class ReplicationStartedFromNone < ReplicationPairState
        def on_replication_stop
          transition ReplicationStopped
        end
        def on_during_take_over
          transition ReplicationLeftRunning
        end
      end

      # replication was stopped after being kept on during
      # takeover. No state changes are permitted
      class ReplicationStoppedTotally < ReplicationPairState
      end

      # replication was stopped between nodes
      class ReplicationStopped < ReplicationPairState
        def on_star_start
          transition StarStarted
        end
      end

      # we've started replica building
      class StarStarted < ReplicationPairState
        def on_star_stop
          transition StarStopped
        end

        def on_during_take_over
          self
        end
      end

      # we've stopped replica building and nothing else happened since
      # then
      class StarStopped < ReplicationPairState
        def on_replication_start
          transition ReplicationStarted
        end

        def on_take_over_start
          transition TakeOverStarted
        end
      end

      # we've started takeover
      class TakeOverStarted < ReplicationPairState
        def on_take_over_done
          transition TakeOverDone
        end
      end

      # takeover is done
      class TakeOverDone < ReplicationPairState
      end

      # replication started or resumed
      class ReplicationStarted < ReplicationPairState
      end
    end

    class ReplicationsMap
      include Event
      attr_reader :replications

      def initialize
        @replications = {}
      end

      def ensure_pair(src, dst)
        key = [src, dst]
        @replications[key] ||= ReplicationPairState.initial(src, dst)
      end

      def handle_transition(sym, src, dst, ev)
        key = [src, dst]
        current_state = @replications[key]
        unless current_state
          pp @replications
          raise "#{ev.raw_hash.pretty_inspect} hits nothing between pair of nodes #{src}-#{dst}\n#{ev.task}"
        end
        catch(:illegal_transition) do
          rv = current_state.send(sym)
          unless rv
            pp @replications
            pp "#{sym}, #{src}, #{dst}, #{ev.raw_hash.pretty_inspect}"
            raise
          end
          rv.associate_event(ev.raw_hash, sym)
          return @replications[key] = rv
        end
        # :illegal_transition was thrown
        raise "illegal event #{ev.raw_hash.pretty_inspect} in state replication state #{current_state.pretty_inspect}"
      end

      def prepare_pairs!(events)
        events.each do |ev|
          case ev
          when VBucketReplicationStart
            ensure_pair(ev.src, ev.dst)
          when VBucketReplicationStop
            sev = ev.migrator_start
            ensure_pair(sev.src, sev.dst)
          when EbucketmigratorStart
            ensure_pair(ev.src, ev.dst)
          when EbucketmigratorTerminate
            sev = ev.task.start_event
            # puts "ensuring pair #{sev.src}-#{sev.dst}"
            ensure_pair(sev.src, sev.dst)
            raise unless @replications[[sev.src, sev.dst]]
          end
        end
      end

      def handle_event!(ev)
        case ev
        when VBucketReplicationStart
          handle_transition(:on_replication_start, ev.src, ev.dst, ev)
        when VBucketReplicationStop
          sev = ev.migrator_start
          handle_transition(:on_replication_stop, ev.src, ev.dst, ev)
        when EbucketmigratorStart
          case ev.task
          when TakeoverMigratorTask
            handle_transition(:on_take_over_start, ev.src, ev.dst, ev)
            @replications.each_key do |(src, dst)|
              next if src == ev.src && dst == ev.dst
              handle_transition(:on_during_take_over, src, dst, ev)
            end
          when ReplicaBuildingMigratorTask
            handle_transition(:on_star_start, ev.src, ev.dst, ev)
          else
            raise
          end
        when EbucketmigratorTerminate
          task = ev.task
          sev = task.start_event
          case ev.task
          when TakeoverMigratorTask
            # lets fire on_during_take_over yet again just in case
            @replications.each_key do |(src, dst)|
              next if src == sev.src && dst == sev.dst
              handle_transition(:on_during_take_over, src, dst, ev)
            end
            handle_transition(:on_take_over_done, sev.src, sev.dst, ev)
          when ReplicaBuildingMigratorTask
            handle_transition(:on_star_stop, sev.src, sev.dst, ev)
          else
            raise
          end
        end
      end

      def assert_valid_final_state!(pre_chain, post_chain)
        pre_chain = pre_chain.dup
        post_chain = post_chain.dup

        pre_chain.delete "undefined"
        pre_chain.delete ""
        post_chain.delete "undefined"
        post_chain.delete ""

        # TODO: handle completely empty pre_chain (i.e. after
        # data-loss-failover)

        expected_pairs = pre_chain.each_cons(2).to_set
        expected_pairs += post_chain.each_cons(2).to_set
        expected_pairs += (post_chain.map {|into| [pre_chain[0], into]} - [[pre_chain[0], pre_chain[0]]]).to_set

        expected_pairs.each {|(src, dst)| ensure_pair(src, dst)}

        @replications.each_value {|state| state.assert_valid_end_state!(pre_chain, post_chain)}
      end
    end

    def assert_all_events_ok!
      raise "missing start event" unless self.start_event
      raise "missing end event" unless self.end_event
      raise "missing map update event" unless self.map_update_event

      all_events = self.all_events
      # exclude temporary replication changes due to vbucket filter
      # changes
      unrelated_replication_changes = []
      prev_per_pair = {}
      all_events.each do |ev|
        next unless ev.kind_of?(VBucketReplicationStop) ||
          ev.kind_of?(VBucketReplicationStart) ||
          ev.kind_of?(EbucketmigratorStart)

        pair = [ev.src, ev.dst]
        prev = prev_per_pair[pair]
        prev_per_pair[pair] = ev
        if prev.kind_of?(VBucketReplicationStop) && ev.kind_of?(VBucketReplicationStart) &&
            ev.migrator_start.task.previous_of_same_name == prev.migrator_terminate.task
          # if nothing was done between this pair of nodes in between
          # stop/start, then we assume this was just side effect of
          # vbucket filter change between this pair of nodes (not
          # really affecting this vbucket)
          unrelated_replication_changes << prev
          unrelated_replication_changes << ev
        end
      end
      all_events -= unrelated_replication_changes.to_a
      if self.vbucket == 0
        puts "unrelated_replication_changes:"
        pp unrelated_replication_changes.map(&:raw_hash)
        puts "events left:"
        pp all_events.map(&:raw_hash)
      end

      raise if all_events.size < 2 # at least start & end

      # doesn't hold if we start some replication before start_event
      # Also doesn't hold because it's possible to observe stop
      # replication event that was made after start_event actually
      # before that start_event because of queuing
      #
      # assert_equals! self.start_event, all_events[0], "start event is first", :to_s do
      #   puts "first_event"
      #   pp all_events[0].raw_hash
      #   puts "start_event"
      #   pp self.start_event.raw_hash
      #   pp all_events.include?(self.start_event)
      #   pp self.events.include?(self.start_event)
      #   pp @events.include?(self.start_event)
      # end

      raise unless self.end_event
      raise unless self.events.include?(self.end_event)
      # TODO: find out why this does not hold
      #
      # assert_equals! [self.end_event, self.map_update_event].sort_by(&:raw_ts), all_events[-2..-1], "end event is last", :to_s do
      #   puts "last_events:"
      #   all_events[-5..-1].each do |ev|
      #     pp ev.raw_hash
      #   end
      # end

      all_events -= [self.start_event, self.map_update_event, self.end_event]

      rep_map = ReplicationsMap.new

      rep_map.prepare_pairs!(all_events)
      # pp rep_map

      all_events.each do |ev|
        rep_map.handle_event!(ev)
      end

      puts "vbucket: #{self.vbucket}"
      rep_map.assert_valid_final_state!(self.map_update_event.chain_before, self.map_update_event.chain_after)
    end

    def self.identify_related_events(master_task, events)
      events.each do |ev|
        case ev
        when VbucketMoverStart
          (vbucket_subtask = master_task.bucket_subtask(ev.bucket).vbucket_subtask(ev.vbucket)).claim_start_event(ev)
          raise unless vbucket_subtask.start_event == ev
          raise unless vbucket_subtask.events.include?(ev)
        when VbucketMoveDone
          master_task.bucket_subtask(ev.bucket).vbucket_subtask(ev.vbucket).claim_end_event(ev)
        when EbucketmigratorStart
          migrator_task = ev.task
          case migrator_task
          when TakeoverMigratorTask
            master_task.bucket_subtask(ev.bucket).vbucket_subtask(ev.vbuckets[0]).claim_takeover_subtask(migrator_task)
          when ReplicaBuildingMigratorTask
            master_task.bucket_subtask(ev.bucket).vbucket_subtask(ev.vbuckets[0]).note_replica_building_task(migrator_task)
          when ReplicationRunTask
            # TODO: when adding ebucketmigrator args to terminate we
            # will be able to see "just terminate" (without start)
            # replication run tasks. So adapt this code accordingly
            #
            # Replication run tasks are attached
            # to bucket rebalance task directly
            bucket_subtask = master_task.bucket_subtask(ev.bucket)
            migrator_task.parent_task! bucket_subtask

            all_vbuckets = ev.vbuckets
            start_events = all_vbuckets.map do |vbucket|
              VBucketReplicationStart.new(vbucket, ev, false)
            end
            not_ready_vbuckets_event = migrator_task.not_ready_vbuckets_event
            if not_ready_vbuckets_event
              not_ready_vbuckets = not_ready_vbuckets_event.vbuckets
              all_vbuckets += not_ready_vbuckets
              start_events += not_ready_vbuckets.map do |vbucket|
                VBucketReplicationStart.new(vbucket, ev, true)
              end
            end
            terminate_event = migrator_task.end_event
            stop_events = []
            if terminate_event
              stop_events = all_vbuckets.map do |vbucket|
                VBucketReplicationStop.new(vbucket, terminate_event)
              end
            end

            (start_events + stop_events).each do |ev|
              bucket_subtask.vbucket_subtask(ev.vbucket).claim_event(ev)
            end
          else
            raise
          end
        end
      end
    end
  end

  class MigratorRunTask < BaseTask
    event_slot :deregister_event, :start_event, :end_event, :not_ready_vbuckets_event
    attr_reader :full_name
    attr_accessor :reused_tap_name

    def assert_full_name(event)
      full_name = event.full_name
      return if @full_name == full_name
      claim_slot :full_name, full_name
    end

    def claim_deregister_event(ev)
      claim_event_slot :deregister_event, ev
      assert_full_name ev
    end

    def claim_end_event(ev)
      raise unless ev.kind_of?(EbucketmigratorTerminate)
      claim_event_slot :end_event, ev
    end

    def self.from_start_event(start_event)
      name = start_event.name
      klass = nil

      case name
      when /\Areplication_building_([0-9]+)_/
        assert_equals! start_event.vbuckets[0], $1.to_i, "replica building vbucket"
        assert_equals! 1, start_event.vbuckets.size, "single vbucket"
        assert_equals! start_event.takeover, "false", "not takeover"
        klass = ReplicaBuildingMigratorTask
      when /\Arebalance_([0-9]+)\z/
        assert_equals! start_event.vbuckets[0], $1.to_i, "rebalance vbucket"
        assert_equals! 1, start_event.vbuckets.size, "single vbucket"
        assert_equals! start_event.takeover, "true", "takeover"
        klass = TakeoverMigratorTask
      when /\Areplication_/
        assert_equals! start_event.takeover, "false", "not takeover"
        # puts "bad vbuckets #{start_event.inspect}" unless start_event.vbuckets.size > 0
        klass = ReplicationRunTask
      else
        raise "unknown ebucketmigrator name prefix: #{name.inspect}"
      end

      rv = klass.new
      rv.claim_start_event(start_event)
      rv.assert_full_name start_event
      rv
    end

    def self.identify_migrator_tasks(master_task, events)
      by_pid = {}
      events.each do |ev|
        case ev
        when EbucketmigratorStart
          assert_equals! nil, by_pid[ev.pid], "not registered yet"
          by_pid[ev.pid] = MigratorRunTask.from_start_event(ev)
        end
      end

      # this are from [bucket, src, tap_name] => task
      unregistered_names = {}
      by_name = {}

      events.each do |ev|
        case ev
        when EbucketmigratorStart
          task = by_pid[ev.pid]
          raise unless task
          task.reused_tap_name = !unregistered_names.has_key?(ev.full_name)
          unregistered_names.delete ev.full_name
          if (prev_same_name = by_name[ev.full_name])
            task.identify_previous_with_same_name(prev_same_name)
          end
          by_name[ev.full_name] = task
        when DeregisterTapName
          prev_task = by_name[ev.full_name]
          if prev_task
            assert_equals!(true, prev_task.end_event != nil, "migrator ended before unregistering") do
              puts "ev: #{ev.pretty_inspect}"
              puts "prev_task: #{prev_task.pretty_inspect}"
            end
          end
          unregistered_names[ev.full_name] = true
          if (task = by_pid[ev.pid])
            # if deregister is from ebucketmigrator process we claim it
            task.claim_deregister_event ev
          else
            # but some deregister_tap_name events are from single vbucket mover processes
            if !prev_task
              # TODO: if we need this, then implement
              master_task.note_unrelated_deregister_tap_name
            else
              prev_task.claim_final_deregister(ev)
            end
          end
        when EbucketmigratorTerminate
          task = by_pid[ev.pid]
          if task
            task.claim_end_event(ev)
            if (new_task = by_name[task.start_event.full_name]) != task
              # if we terminated _after_ new guy for same name was
              # created, we're changing filter. Assert at least tap name reuse
              assert_equals! true, new_task.reused_tap_name, "expecting tap reuse for vbucket filter change" do
                puts "task:"
                pp task
                puts "new_task:"
                pp new_task
              end
            end
          else
            # TODO: implement this
            # puts "unknown terminate"
            # pp ev
            # master_task.note_unknown_ebucketmigrator_terminate(ev)
          end
        when NotReadyVBuckets
          # TODO: consider handling by_pid[ev.pid] being nil
          by_pid[ev.pid].claim_not_ready_vbuckets_event(ev)
          # puts "saw not-ready-vbuckets for task #{by_pid[ev.pid].pretty_inspect}"
        end
      end

      by_pid
    end
  end

  class ReplicaBuildingMigratorTask < MigratorRunTask
    event_slot :final_deregister

    def claim_final_deregister(ev)
      claim_event_slot :final_deregister, ev
      assert_full_name ev
    end
  end

  class TakeoverMigratorTask < MigratorRunTask
  end

  class ReplicationRunTask < MigratorRunTask
    claimable_slot :previous_of_same_name
    claimable_slot :next_of_same_name

    def identify_previous_with_same_name(prev_same_name)
      claim_previous_of_same_name prev_same_name
      prev_same_name.claim_next_of_same_name(self)

      my_start = self.start_event
      other_start = prev_same_name.start_event
      assert_equals! [my_start.src, my_start.dst], [other_start.src, other_start.dst], "prev of same name applies to same pair of nodes"
    end
  end

  def process_vbucket_subtask(task, seen_per_vbucket_tasks)
    return if seen_per_vbucket_tasks.include? task
    seen_per_vbucket_tasks << task
    task.assert_all_events_ok!
  end
  module_function :process_vbucket_subtask

  def verify_bucket_rebalance(bucket_task)
    events = bucket_task.all_events
    # TODO: add verification of concurrency constraints
    # current_movers_per_source = {}

    # TODO: allow janitor run as part of bucket rebalance
    seen_per_vbucket_tasks = Set.new
    events.each do |ev|
      task = ev.task
      case task
      when bucket_task
        next unless bucket_task.kind_of?(UpdateFastForwardMap)
        raise "ev: #{ev.raw_hash.pretty_inspect}"
      when MigratorRunTask
        expected_tap_reuse = !!(task.kind_of?(ReplicationRunTask) && task.previous_of_same_name && !(task.previous_of_same_name.not_ready_vbuckets_event && task.previous_of_same_name.end_event.reason == "normal"))
        if expected_tap_reuse  # works only in true case because of
                               # unidentified ebucketmigrator
                               # terminate events
          assert_equals! expected_tap_reuse, task.reused_tap_name, "tap reuse as expected" do
            puts "prev"
            pp task.previous_of_same_name.events.sort_by(&:raw_ts).map(&:raw_hash)
            puts "this task"
            pp task.events.sort_by(&:raw_ts).map(&:raw_hash)
          end
        end
        unless task.kind_of?(ReplicationRunTask)
          raise unless (task = task.parent_task).kind_of?(VBucketMovementTask)
          process_vbucket_subtask(task, seen_per_vbucket_tasks)
        end
      when VBucketMovementTask
        # ignore
      else
        raise "task: #{task}, #{ev}"
      end
    end
  end
  module_function :verify_bucket_rebalance

  def identify_and_verify_tasks(events)
    raise unless events[0].kind_of? RebalanceStart
    raise unless events[-1].kind_of? RebalanceEnd
    raise unless events[0].pid == events[-1].pid

    master_task = RebalanceTask.new(events[0], events[-1])
    BucketRebalanceTask.perform_claim_ff_update_pass(master_task, events)
    BucketRebalanceTask.perform_map_update_pass(master_task, events)

    migrators_by_pid = MigratorRunTask.identify_migrator_tasks(master_task, events)

    VBucketMovementTask.identify_related_events(master_task, events)

    events, unknown_events = events.partition {|ev| ev.task}
    unless unknown_events.size == 0
      puts("Found some unidentified event".pluralize(unknown_events.size))
      pp unknown_events
    end

    # NOTE: don't forget that some vbuckets may be not moved at all

    # TODO: assert everything that needs to be found is found
    # TODO: double check all cases have else when needed
    # TODO: why we don't have vbucket deletions in master events ?
    # TODO: verify we don't have not-ready-vbuckets in non-replication streams
    # TODO: consider checking node where replication runs

    master_task.freeze_structure!

    bucket_rebalances = master_task.subtasks.sort_by {|t| t.first_event.raw_ts}
    bucket_rebalances_by_last_event = master_task.subtasks.sort_by {|t| t.last_event.raw_ts}
    # TODO: this is not quite correct (misses some intersections). fix
    assert_equals! bucket_rebalances, bucket_rebalances_by_last_event, "bucket rebalances don't intersect in time"

    bucket_rebalances.each do |bucket_task|
      verify_bucket_rebalance(bucket_task)
    end

    # TODO: check vbucket maps and identify swap_rebalance or maybe
    # any optimal vbucket movements

    # TODO: dont forget to check named tap reuse

    # TODO: consider checking one per source constraint

    # TODO: zero data loss needs to be checked
  end
  module_function :identify_and_verify_tasks

end

# pp IdentifyTasks::RebalanceTask.new(1, 2)

raw_events = IO.readlines(ARGV[0] || raise("need filename")).map {|l| Event.from_hash(JSON.parse(l))}

IdentifyTasks.identify_and_verify_tasks(raw_events)

