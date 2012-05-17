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

raw_events = IO.readlines(ARGV[0] || raise("need filename")).map {|l| Event.maybe_from_hash(JSON.parse(l))}.compact

include Event

events_by_pid = {}
name2pids = {}
raw_events.each do |ev|
  next unless ev.kind_of?(EbucketmigratorStart)
  events_by_pid[ev.pid] = [ev]
  (name2pids[ev.full_name] ||= []) << ev.pid
end


raw_events.each do |ev|
  next if ev.kind_of?(EbucketmigratorStart)
  next unless ev.respond_to?(:pid) && events_by_pid.has_key?(ev.pid)
  events_by_pid[ev.pid] << ev
end

notable_pids = []

raw_events.each do |ev|
  next unless ev.kind_of?(EbucketmigratorStart)
  next unless ev.vbuckets.include?(31)
  notable_pids << ev.pid
end

all_interesting_events = Set.new

notable_pids.each do |pid|
  all_interesting_events += events_by_pid[pid].to_set
  related_pids = name2pids[events_by_pid[pid][0].full_name]
  related_pids.each do |rpid|
    all_interesting_events += events_by_pid[rpid].to_set
  end
end

raw_events.each do |ev|
  next unless ev.kind_of?(VbucketMoverStart) || ev.kind_of?(VbucketMoveDone)
  next unless ev.vbucket == 31
  all_interesting_events << ev
end

all_interesting_events = all_interesting_events.to_a.sort_by(&:raw_ts)

pp all_interesting_events.map(&:raw_hash)
