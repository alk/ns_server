#!/bin/sh

if test "x$1" == "x-c"; then
    make conf-clean
fi

reloader -T 30 -s KILL -u 3000 -d 8080 -w '**/*.erl' -w 'deps/menelaus/priv/js/**/*.js' -- sh -c 'make -j3 fast-rebuild && ./start_shell.sh -noshell'
