#!/usr/bin/ruby

Dir.chdir(File.dirname($0))

begin
  pid = IO.read('../pserver.pid').strip.to_i
  Process.kill('CHLD', pid)
  exit(0)
rescue Errno::ENOENT, Errno::ESRCH
  # nothing
end

if ARGV.size == 0
  puts "no .pid found to kill"
  exit(1)
end

File.open('../pserver.pid', 'w') {|f| f << Process.pid}

$rd, $wr = IO.pipe

$die_sig = false
$dont_kill = false

trap('CHLD') {$wr << 'c'}
trap('TERM') {$die_sig = true; $wr << 'c'}
trap('INT') {$die_sig = true; $dont_kill = true; $wr << 'c'}

def spawn_child!
  $child = fork do
    exec(*ARGV)
  end
  diag "child is #{$child}"
end

def diag(msg)
  system "notify-send -t 2000 pserver '#{msg}'"
end

$tcattrs = begin
             require 'rubygems'
             require 'termios'
             puts "Will restore terminal attrs after child"
             Termios.tcgetattr(STDIN)
           rescue LoadError, Errno::ENOTTY
           end

while !$die_sig
  begin
    while true
      $rd.read_nonblock(4096)
    end
  rescue Errno::EAGAIN
  end

  spawn_child!

  $rd.readpartial(1)

  unless $dont_kill
    Process.kill('INT', $child) rescue nil
    killer_thread = Thread.new do
      sleep 3
      Process.kill('TERM', $child) rescue nil
    end
    Process.wait($child)
    killer_thread.kill
    $child = nil
  end

  begin
    Process.wait($child) if $child
  rescue Exception
    diag "child waiting failed!"
  ensure
    if $tcattrs
      Termios.tcsetattr(STDIN, Termios::TCSANOW, $tcattrs)
    end
  end
end
