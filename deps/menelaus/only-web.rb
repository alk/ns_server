#!/usr/bin/ruby

require 'rubygems'
require 'sinatra'
require 'active_support'
require 'pp'

Dir.chdir(File.dirname($0))

$DOCROOT = 'priv/public'

def sh(cmd)
  puts "# #{cmd}"
  ok = system cmd
  unless ok
    str = "FAILED! #{$?.inspect}"
    puts str
    raise str
  end
end

$DEVMODE = true

class Middleware
  def initialize(app)
    @app = app
  end

  $JS_ESCAPE_MAP = { '\\' => '\\\\', '</' => '<\/', "\r\n" => '\n', "\n" => '\n', "\r" => '\n', '"' => '\\"', "'" => "\\'" }
  def escape_javascript(javascript)
    if javascript
      javascript.gsub(/(\\|<\/|\r\n|[\n\r"'])/) { $JS_ESCAPE_MAP[$1] }
    else
      ''
    end
  end

  def call(env)
    req = Rack::Request.new(env)
    if req.path_info == "/index.html"
      replacement = "/js/t-all.js\""
      if $DEVMODE
        files = IO.readlines('priv/js/app.js').grep(%r|//= require <(.*)>$|) {|_d| $1}
        files << "app.js" << "hooks.js"
        files = files.map {|f| "/js/#{f}"}
        replacement = files.join("\"></script><script src=\"") << "\""
      end
      text = IO.read($DOCROOT + "/index.html").gsub(Regexp.compile(Regexp.escape("/js/all.js\"")), replacement)
      return [200, {'Content-Type' => 'text/html; charset=utf-8'}, [text]]
    end

    @app.call(env)
  end
end

helpers do
  def auth_credentials
    @auth ||=  Rack::Auth::Basic::Request.new(request.env)
    if @auth.provided?
      @auth.credentials
    end
  end
end

use Middleware

set :public, $DOCROOT

get "/" do
  redirect "/index.html"
end

get "/test_auth" do
  user, pwd = *auth_credentials
  if user != 'admin' || pwd != 'admin'
#    response['WWW-Authenticate'] = 'Basic realm="api"'
    response['Cache-Control'] = 'no-cache must-revalidate'
    throw(:halt, 401)
  end
  "OK"
end

if ARGV.size == 0
  name = "./" + File.basename($0)
  exec name, "-p", "8080" #, "-s", "webrick"
end
