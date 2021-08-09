# how do the other test drivers access the executable?
ROOT = ../../../..

#DDLOG = $(ROOT)/ddlog/bin/ddlog
DDLOG=${HOME}/.local/bin/ddlog

all: run

dred: run_dred

compiler:
	(cd $(ROOT) ;  stack build && stack install)

program: 
	$(DDLOG) --no-staticlib --dynlib --action compile -L$(ROOT)/lib -L.. --d3log -i mini_skyline.dl

compile: 
	(cd mini_skyline_ddlog ; cargo build --features distribution)

start: 
	./mini_skyline_ddlog/target/debug/mini_skyline_cli -c -m -i mini_skyline.json 

program_dred: 
	$(DDLOG) --no-staticlib --dynlib --action compile -L$(ROOT)/lib -L.. --d3log -i dred_test.dl

compile_dred: 
	(cd dred_test_ddlog ; cargo build --features distribution)

start_dred:
	./dred_test_ddlog/target/debug/dred_test_cli -i dred_test.json 

run_dred: program_dred compile_dred start_dred

inc: compile start
run: program compile start	

clean:
	rm -rf *_ddlog
