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

#include <glog/logging.h>

#include "driver.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;


#define AS_MAX_LOG_STR 1024
static bool glog_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...);


namespace gkvs {

    class AerospikeDriver final : public Driver {

    public:

        explicit AerospikeDriver(const json &conf, const std::string &lua_path) : Driver() {

            as_log_set_callback(glog_callback);


            // Initialize default lua configuration.
            as_config_lua lua;
            as_config_lua_init(&lua);

            if (!lua_path.empty()) {

                if (lua_path.length() < (AS_CONFIG_PATH_MAX_SIZE - 1)) {
                    strcpy(lua.user_path, lua_path.data());
                } else {
                    LOG(ERROR) << "lua_path is too long: " << lua_path;
                }

            }
            else {
                LOG(INFO) << "lua_path is empty ";
            }

            // Initialize global lua configuration.
            aerospike_init_lua(&lua);

            as_config config;
            as_config_init(&config);

            json cluster = conf["cluster"];

            std::string host = cluster["host"].get<std::string>();
            int port = cluster["port"].get<int>();

            if (! as_config_add_hosts(&config, host.data(), port)) {
                LOG(ERROR) << "invalid host: " << host;
                throw std::invalid_argument( "invalid host" );
            }

            std::string username = cluster["username"].get<std::string>();
            std::string password = cluster["password"].get<std::string>();

            as_config_set_user(&config, username.data(), password.data());

            //memcpy(&config.tls, &g_tls, sizeof(as_config_tls));
            //config.auth_mode = g_auth_mode;

            aerospike_init(&_as, &config);

            as_error err;

            if (aerospike_connect(&_as, &err) != AEROSPIKE_OK) {
                LOG(ERROR) << "aerospike_connect code: " << err.code << ", message:" << err.message << host;
                throw std::invalid_argument( "aerospike_connect" );
            }

        }

        ~AerospikeDriver() override {

            as_error err;

            // Disconnect from the database cluster and clean up the aerospike object.
            aerospike_close(&_as, &err);
            aerospike_destroy(&_as);

            std::cout << "Graceful shutdown aerospike connection" << std::endl;

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

    Driver* create_aerospike_driver(const json &conf, const std::string &lua_path) {
        return new AerospikeDriver(conf, lua_path);
    }


}

static bool glog_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{

    va_list ap;
    va_start(ap, fmt);
    // allocate in heap to avoid stack overflow by untrusted vsnprintf function
    char *str = new char[AS_MAX_LOG_STR];
    int affected = vsnprintf(str, AS_MAX_LOG_STR-1, fmt, ap);
    if (affected > 0) {

        switch(level) {

            case AS_LOG_LEVEL_ERROR:
                LOG(ERROR) << str << std::endl;
                break;

            case AS_LOG_LEVEL_WARN:
                LOG(WARNING) << str << std::endl;
                break;

            case AS_LOG_LEVEL_INFO:
                LOG(INFO) << str << std::endl;
                break;

            case AS_LOG_LEVEL_DEBUG:
                DLOG(INFO) << str << std::endl;
                break;

            case AS_LOG_LEVEL_TRACE:
                VLOG(0) << str << std::endl;
                break;

            default:
                LOG(ERROR) << "unknown log level: " << level << ", msg: " << str << std::endl;
                break;

        }

    }
    va_end(ap);
    delete [] str;
    return true;
}

