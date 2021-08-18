# how do the other test drivers access the executable?
ROOT = ../../../..

#DDLOG = $(ROOT)/ddlog/bin/ddlog
DDLOG=${HOME}/.local/bin/ddlog

all: run

compiler:
	(cd $(ROOT) ;  stack build && stack install)

PROGRAM=mini_skyline

program: 
	$(DDLOG) --no-staticlib --dynlib --action compile -L$(ROOT)/lib -L.. --d3log -i $(PROGRAM).dl

compile: program
	(cd $(PROGRAM)_ddlog ; cargo build --features distribution)

# -r n4traceout
start: 
	./$(PROGRAM)_ddlog/target/debug/$(PROGRAM)_cli -c -m -i $(PROGRAM).json 


inc: compile start
run: program compile start	

clean:
	rm -rf *_ddlog
