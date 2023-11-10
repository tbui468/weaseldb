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

GCC_CXXFLAGS = -DMESSAGE='"Compiled with GCC"'
CLANG_CXXFLAGS = -DMESSAGE='"Compiled with Clang"'
UNKNOWN_CXXFLAGS = -DMESSAGE='"Compiled with an unknown compiler"'

ifeq ($(CXX),g++)
  CXXFLAGS += $(GCC_CXXFLAGS)
else ifeq ($(CXX),clang)
  CXXFLAGS += $(CLANG_CXXFLAGS)
else
  CXXFLAGS += $(UNKNOWN_CXXFLAGS)
endif

CFLAGS += -Wstrict-prototypes
LDFLAGS="-Wl,-rpath,../../libtorch/lib"
CXXFLAGS += -Wall

.PHONY: clean librocksdb

#main: librocksdb main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc index.cc nn.cc
	#$(CXX) $(CXXFLAGS) $(LDFLAGS) $(TORCH_CXX_FLAGS) main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc index.cc nn.cc -o wsldb -L../../libtorch/lib -lc10 -ltorch_cpu ../../rocksdb/librocksdb.a -I../../rocksdb/include -I../include -I../../libtorch/include/torch/csrc/api/include -I../../libtorch/include -O2 -std=c++17 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

main: librocksdb main.cc tokenizer.cc parser.cc token.cc schema.cc server.cc storage.cc index.cc executor.cc analyzer.cc attribute.cc txn.cc
	$(CXX) $(CXXFLAGS) main.cc tokenizer.cc parser.cc token.cc schema.cc server.cc storage.cc index.cc executor.cc analyzer.cc attribute.cc txn.cc -o wsldb ../../rocksdb/librocksdb.a -I../../rocksdb/include -I../include -O1 -std=c++17 -fuse-ld=lld $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./main

#librocksdb:
#	cd .. && $(MAKE) static_lib
