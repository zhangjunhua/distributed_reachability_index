CCOMPILE=mpic++
CPPFLAGS= -O2 -std=c++11 -g -Wall

deps = $(wildcard utils/*.h *.h *.cpp)

target=drl

All: $(target)

$(target): $(deps)
	$(CCOMPILE) run.cpp $(LIB) $(LDFLAGS) $(CPPFLAGS) -o $(target)

clean:
	rm $(target)
