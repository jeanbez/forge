CC=mpicc
CFLAGS=-c -Wall #-DDEBUG
LDFLAGS=-lagios -lgsl -lgslcblas -lm -L. -ljsmn
SOURCES=main.c fwd_list.c
OBJECTS=$(SOURCES:.c=.o)
EXECUTABLE=fwd-sim

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(OBJECTS) -o $@ $(LDFLAGS)

.c.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf *.o $(EXECUTABLE)
