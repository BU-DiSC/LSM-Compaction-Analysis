include ../../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
endif

.PHONY: clean librocksdb

all: working_version 

working_version: librocksdb working_version.cc emu_environment.cc stats.cc aux_time.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ emu_environment.cc stats.cc aux_time.cc ../../librocksdb.a -I../../include -I../.. -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

working_version_debug: librocksdb_debug working_version.cc emu_environment.cc stats.cc aux_time.cc
	$(CXX) $(CXXFLAGS) working_version.cc -o$@ emu_environment.cc stats.cc aux_time.cc ../../librocksdb_debug.a -I../../include -I../.. -O0 -ggdb -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf working_version working_version_debug working_version_debug.* 

librocksdb:
	cd ../.. && $(MAKE) static_lib

librocksdb_debug:
	cd ../.. && $(MAKE) dbg_hack
