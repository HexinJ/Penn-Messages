TARGETS = chatserver chatclient

all: $(TARGETS)

%.o: %.cc
	g++ $^ -c -g -o $@

chatserver: chatserver.o
	g++ $^ -o $@ -g

chatclient: chatclient.o
	g++ $^ -o $@ -g

pack:
	rm -f submit-hw3.zip
	zip -r submit-hw3.zip README Makefile *.c* *.h*

clean::
	rm -fv $(TARGETS) *~ *.o submit-hw3.zip

realclean:: clean
	rm -fv submit-hw3.zip
