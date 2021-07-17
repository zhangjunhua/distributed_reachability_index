CCOMPILE=mpic++
#CPPFLAGS= -O2 -std=c++11 -g -Wall -Weffc++ -Wextra -Wsign-conversion
CPPFLAGS= -O2 -std=c++11 -g -Wall
#LDFLAGS = -pthread

deps = $(wildcard basic/*.h utils/*.h reachO3/*.h reachO0/*.h reachO4m1/*.h reachO3new/*.h reachO3a/*.h reachO5/*h reachO4/*.h reachO4b/*.h reachO1/*.h reachO2a/*.h *.h *.cpp)
#target=/home/junhzhan/bin/dist_reach
target=/home/junhzhan/bin/drl
#target=/home/junhzhan/bin/dist_reach2
target2=/home/junhzhan/bin/drl2

t1: $(target)

t2: $(target2)

$(target): $(deps)
	$(CCOMPILE) run.cpp $(LIB) $(LDFLAGS) $(CPPFLAGS) -o $(target)

$(target2): $(deps)
	$(CCOMPILE) run.cpp $(LIB) $(LDFLAGS) $(CPPFLAGS) -o $(target2)

clean:
	$(target2)
