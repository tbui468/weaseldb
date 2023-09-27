include ../../rocksdb/make_config.mk

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

CFLAGS += -Wstrict-prototypes

CXXFLAGS += -Wall

.PHONY: clean librocksdb

main: librocksdb main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc
	$(CXX) $(CXXFLAGS) main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc -o wsldb ../../rocksdb/librocksdb.a -I../../rocksdb/include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./main

#librocksdb:
#	cd .. && $(MAKE) static_lib
