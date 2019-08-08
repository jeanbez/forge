CC=mpicc
CFLAGS=-c -Wall -g #-DDEBUG
LDFLAGS=-lagios -lgsl -lgslcblas -lm -L. -ljsmn -lpthread
SOURCES=main.c jsmn.c log.c fwd_list.c
OBJECTS=$(SOURCES:.c=.o)
EXECUTABLE=fwd-sim

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(OBJECTS) -o $@ $(LDFLAGS)

.c.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf *.o $(EXECUTABLE)
