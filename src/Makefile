#
# Copyright 2018-present GKVS authors.
# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# brew openssl requirement in env
# PKG_CONFIG_PATH=/usr/local/opt/openssl/lib/pkgconfig

HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
C = gcc
CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc openssl`
CPPFLAGS += -I../modules/nlohmann_json/single_include
CXXFLAGS += -std=c++11 -O3 -Wc++11-extensions
LUA_UNK_FLAG=-Wl,-E
LUA_ALL_FLAGS=$(shell pkg-config --libs luajit --static)
LUA_FLAGS=$(subst $(LUA_UNK_FLAG),, $(LUA_ALL_FLAGS))

ifeq ($(SYSTEM),Darwin)
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++ libglog gflags openssl hiredis msgpack --static`\
           -ldl -v
AS_TARGET=Darwin-x86_64
else
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++ libglog gflags openssl hiredis msgpack --static`\
           -Wl,--no-as-needed -Wl,--as-needed\
           -ldl
AS_TARGET=Linux-x86_64
endif
CPPFLAGS += -I../modules/aerospike-client-c/target/$(SYSTEM)-x86_64/include -I../modules/aerospike-client-c/modules/common/src/include
PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`
AEROFLAGS = -DAS_USE_LIBEVENT -levent -levent_pthreads -lssl -lcrypto -lm -lz ../modules/aerospike-client-c/target/$(AS_TARGET)/lib/libaerospike.a

PROJECT=gkvs
PROTOS_PATH = ../protos

vpath %.proto $(PROTOS_PATH)

all: system-check $(PROJECT)-cli $(PROJECT)

$(PROJECT)-cli: $(PROJECT).pb.o $(PROJECT).grpc.pb.o $(PROJECT)_client.o helper.o
	$(CXX) $^ $(LDFLAGS) -o $@

$(PROJECT): $(PROJECT).pb.o $(PROJECT).grpc.pb.o $(PROJECT)_server.o helper.o as_driver.o as_helper.o as_driver_tests.o redis_driver.o crypto.o script.cc
	$(CXX) $^ $(LDFLAGS) $(AEROFLAGS) $(LUA_FLAGS) -o $@

#crypto.o: crypto.cc
#	$(CXX) $^ -c -o $@

%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h $(PROJECT)-cli $(PROJECT)


# The following is to test your system and ensure a smoother experience.
# They are by no means necessary to actually compile a grpc-enabled software.

PROTOC_CMD = which $(PROTOC)
PROTOC_CHECK_CMD = $(PROTOC) --version | grep -q libprotoc.3
PLUGIN_CHECK_CMD = which $(GRPC_CPP_PLUGIN)
HAS_PROTOC = $(shell $(PROTOC_CMD) > /dev/null && echo true || echo false)
ifeq ($(HAS_PROTOC),true)
HAS_VALID_PROTOC = $(shell $(PROTOC_CHECK_CMD) 2> /dev/null && echo true || echo false)
endif
HAS_PLUGIN = $(shell $(PLUGIN_CHECK_CMD) > /dev/null && echo true || echo false)

SYSTEM_OK = false
ifeq ($(HAS_VALID_PROTOC),true)
ifeq ($(HAS_PLUGIN),true)
SYSTEM_OK = true
endif
endif

system-check:
ifneq ($(HAS_VALID_PROTOC),true)
	@echo " DEPENDENCY ERROR"
	@echo
	@echo "You don't have protoc 3.0.0 installed in your path."
	@echo "Please install Google protocol buffers 3.0.0 and its compiler."
	@echo "You can find it here:"
	@echo
	@echo "   https://github.com/google/protobuf/releases/tag/v3.0.0"
	@echo
	@echo "Here is what I get when trying to evaluate your version of protoc:"
	@echo
	-$(PROTOC) --version
	@echo
	@echo
endif
ifneq ($(HAS_PLUGIN),true)
	@echo " DEPENDENCY ERROR"
	@echo
	@echo "You don't have the grpc c++ protobuf plugin installed in your path."
	@echo "Please install grpc. You can find it here:"
	@echo
	@echo "   https://github.com/grpc/grpc"
	@echo
	@echo "Here is what I get when trying to detect if you have the plugin:"
	@echo
	-which $(GRPC_CPP_PLUGIN)
	@echo
	@echo
endif
ifneq ($(SYSTEM_OK),true)
	@false
endif


show:
	otool -L gkvs
