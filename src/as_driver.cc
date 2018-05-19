/*
 *
 * Copyright 2018 gGKVS authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>

#include "driver.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace gkvs {


    static bool console_log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
    {
        va_list ap;
        va_start(ap, fmt);
        vprintf(fmt, ap);
        printf("\n");
        va_end(ap);
        return true;
    }


    class AerospikeDriver final : public Driver {

    public:

        explicit AerospikeDriver(const std::string &host, uint16_t port, const std::string &lua_path) : Driver() {

            as_log_set_callback(console_log_callback);


            // Initialize default lua configuration.
            as_config_lua lua;
            as_config_lua_init(&lua);

            if (!lua_path.empty()) {

                if (lua_path.length() < (AS_CONFIG_PATH_MAX_SIZE - 1)) {
                    strcpy(lua.user_path, lua_path.data());
                } else {
                    std::cerr << "lua_path is too long: " << lua_path << std::endl;
                }

            }

            // Initialize global lua configuration.
            aerospike_init_lua(&lua);

            as_config config;
            as_config_init(&config);

            if (! as_config_add_hosts(&config, host.data(), port)) {
                std::cerr << "invalid host: " << host << std::endl;
                throw std::invalid_argument( "invalid host" );
            }

            as_config_set_user(&config, "", "");

        }

        ~AerospikeDriver() override {

            as_error err;

            // Disconnect from the database cluster and clean up the aerospike object.
            aerospike_close(&_as, &err);
            aerospike_destroy(&_as);

        }

        void getHead(const ::gkvs::KeyOperation *request, ::gkvs::HeadResult *response) override {

        }

        void multiGetHead(const ::gkvs::BatchKeyOperation *request,
                          ::grpc::ServerWriter<::gkvs::HeadResult> *writer) override {

        }

        void get(const ::gkvs::KeyOperation *request, ::gkvs::RecordResult *response) override {

        }

        void multiGet(const ::gkvs::BatchKeyOperation *request,
                      ::grpc::ServerWriter<::gkvs::RecordResult> *writer) override {

        }

        void scanHead(const ::gkvs::ScanOperation *request, ::grpc::ServerWriter<::gkvs::HeadResult> *writer) override {

        }

        void scan(const ::gkvs::ScanOperation *request, ::grpc::ServerWriter<::gkvs::RecordResult> *writer) override {

        }

        void put(const ::gkvs::PutOperation *request, ::gkvs::Status *response) override {

        }

        void compareAndPut(const ::gkvs::PutOperation *request, ::gkvs::Status *response) override {

        }

        void putAll(::grpc::ServerReaderWriter<::gkvs::Status, ::gkvs::PutOperation> *stream) override {

        }

        void remove(const ::gkvs::KeyOperation *request, ::gkvs::Status *response) override {

        }

        void removeAll(const ::gkvs::BatchKeyOperation *request, ::gkvs::Status *response) override {

        }


    private:

        aerospike _as;

    };


}


