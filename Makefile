# Copyright (c) 2011, Membase, Inc.
# All rights reserved.
SHELL=/bin/sh

EFLAGS=-pa ./ebin ./deps/*/ebin ./deps/*/deps/*/ebin

NS_SERVER_PLT ?= ns_server.plt

TMP_DIR=./tmp

TMP_VER=$(TMP_DIR)/version_num.tmp
TEST_TARGET=start

.PHONY: ebins ebin_app version

all: ebins deps_all

deps_menelaus:
	(cd deps/menelaus && $(MAKE) all)

deps_smtp:
	(cd deps/gen_smtp && $(MAKE) ebins)

deps_all: deps_menelaus deps_smtp

docs:
	erl -noshell -run edoc_run application "'ns_server'" '"."' '[]'

ebins: ebin_app
	test -d ebin || mkdir ebin
	erl -noinput +B $(EFLAGS) -eval 'case make:all() of up_to_date -> halt(0); error -> halt(1) end.'

ebin_app: version
	test -d ebin || mkdir ebin
	sed s/0.0.0/`cat $(TMP_VER)`/g src/ns_server.app.src > ebin/ns_server.app

version:
	test -d $(TMP_DIR) || mkdir $(TMP_DIR)
	git describe | sed s/-/_/g > $(TMP_VER)

bdist: clean ebins deps_all
	(cd .. && tar cf -  \
                          ns_server/collect_info \
                          ns_server/ebin \
                          ns_server/deps/*/ebin \
                          ns_server/deps/*/deps/*/ebin \
                          ns_server/deps/menelaus/priv/public \
                          ns_server/deps/menelaus/deps/erlwsh/priv | gzip -9 -c > \
                          ns_server/ns_server_`cat ns_server/$(TMP_VER)`.tar.gz )
	echo created ns_server_`cat $(TMP_VER)`.tar.gz

clean clean_all:
	@(cd deps/menelaus && $(MAKE) clean)
	@(cd deps/gen_smtp && $(MAKE) clean)
	rm -f $(TMP_VER)
	rm -f $(TMP_DIR)/*.cov.html
	rm -f cov.html
	rm -f erl_crash.dump
	rm -f ns_server_*.tar.gz
	rm -f src/ns_server.app
	rm -rf test/log
	rm -rf ebin

test: test_$(OS)

test_: test_unit test_menelaus

test_Windows_NT: test_unit test_menelaus

test_unit: ebins
	erl $(EFLAGS) -noshell -kernel error_logger silent -shutdown_time 10000 -eval 'application:start(sasl).' -eval "case t:$(TEST_TARGET)() of ok -> init:stop(); _ -> init:stop(1) end."

test_menelaus: deps/menelaus
	(cd deps/menelaus; $(MAKE) test)

# assuming exuberant-ctags
TAGS:
	ctags -eR .

$(NS_SERVER_PLT):
	dialyzer --output_plt $@ --build_plt -pa ebin --apps compiler crypto erts inets kernel mnesia os_mon sasl ssl stdlib xmerl -c deps/gen_smtp/ebin deps/menelaus/deps/mochiweb/ebin deps/menelaus/deps/erlwsh/ebin

dialyzer: all $(NS_SERVER_PLT)
	dialyzer --plt $(NS_SERVER_PLT) -Wunderspecs -pa ebin -c ebin -c deps/menelaus/ebin

Features/Makefile:
	(cd features && ../test/parallellize_features.rb) >features/Makefile

.PHONY : features/Makefile

parallel_cucumber: features/Makefile
	$(MAKE) -k -C features all_branches

ln:
	./ln-memcached

clean-restart: dataclean ln ebins deps_all
	./start_shell.sh

fast-rebuild: ln ebins deps_all
	echo done

clean-rebuild: dataclean ln ebins deps_all
	echo done
