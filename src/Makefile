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
CXXFLAGS += -Wextra
CXXFLAGS += -Wshadow
#CXXFLAGS += -Wpedantic

.PHONY: clean librocksdb

main: link_ml
	rm *.o

link_ml: librocksdb compile_ml
	$(CXX) $(LDFLAGS) $(TORCH_CXX_FLAGS) -Wall -DML *.o -o wsldb -L../../libtorch/lib -lc10 -ltorch_cpu ../../rocksdb/librocksdb.a -I../../rocksdb/include -I../include  -I../../libtorch/include/torch/csrc/api/include -I../../libtorch/include -std=c++17 -fuse-ld=lld $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compile_ml: main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc index.cc executor.cc analyzer.cc attribute.cc txn.cc inference.cc
	$(CXX) $(CXXFLAGS) $(TORCH_CXX_FLAGS) -DML -c main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc index.cc executor.cc analyzer.cc attribute.cc txn.cc inference.cc -isystem../../rocksdb/include -I../include  -isystem../../libtorch/include/torch/csrc/api/include -isystem../../libtorch/include -std=c++17 $(PLATFORM_CXXFLAGS)

#main: librocksdb main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc index.cc executor.cc analyzer.cc attribute.cc txn.cc inference.cc
#	$(CXX) $(CXXFLAGS) $(LDFLAGS) $(TORCH_CXX_FLAGS) -DML main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc index.cc executor.cc analyzer.cc attribute.cc txn.cc inference.cc -o wsldb -L../../libtorch/lib -lc10 -ltorch_cpu ../../rocksdb/librocksdb.a -I../../rocksdb/include -I../include  -I../../libtorch/include/torch/csrc/api/include -I../../libtorch/include -std=c++17 -fuse-ld=lld $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

#no_ml: librocksdb main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc index.cc executor.cc analyzer.cc attribute.cc txn.cc inference.cc
#	$(CXX) $(CXXFLAGS) main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc index.cc executor.cc analyzer.cc attribute.cc txn.cc inference.cc -o wsldb_no_ml ../../rocksdb/librocksdb.a -I../../rocksdb/include -I../include -std=c++17 -fuse-ld=lld $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)


link_no_ml: librocksdb compile_no_ml
	$(CXX) -Wall *.o -o wsldb ../../rocksdb/librocksdb.a -I../../rocksdb/include -I../include -std=c++17 -fuse-ld=lld $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

compile_no_ml: main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc index.cc executor.cc analyzer.cc attribute.cc txn.cc inference.cc
	$(CXX) $(CXXFLAGS) -c main.cc tokenizer.cc parser.cc token.cc table.cc server.cc storage.cc index.cc executor.cc analyzer.cc attribute.cc txn.cc inference.cc -isystem../../rocksdb/include -I../include -std=c++17 $(PLATFORM_CXXFLAGS)

no_ml: link_no_ml
	rm *.o

#librocksdb:
#	cd .. && $(MAKE) static_lib
