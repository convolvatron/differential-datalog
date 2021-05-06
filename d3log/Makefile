
EXEC = target/debug/mm3
all: out
DIST = ..
TARGET = d3_supervisor

$(TARGET)_ddlog/types/$(TARGET).rs: d3_supervisor.dl
	ddlog --no-staticlib --dynlib --action compile -L$(DIST)/lib --d3log -i $(TARGET).dl

$(EXEC): $(TARGET)_ddlog/types/$(TARGET).rs src/*.rs 
	cargo build

out: $(EXEC)
	$(EXEC) -n 3

clean:
	rm -rf $(TARGET)_ddlog
