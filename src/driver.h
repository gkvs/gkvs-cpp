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

#pragma once

#include <string>
#include <vector>
#include "gkvs.grpc.pb.h"


namespace gkvs {

    class Driver {

    public:
        Driver() = default;
        virtual ~Driver() = default;
        virtual void get(const KeyOperation* request, ValueResult* response) = 0;
        virtual void multiGet(const BatchKeyOperation *request, BatchValueResult *response) = 0;
        virtual void getAll(::grpc::ServerReaderWriter<::gkvs::ValueResult, KeyOperation> *stream) = 0;
        virtual void scan(const ScanOperation* request, ::grpc::ServerWriter< ValueResult>* writer) = 0;
        virtual void put(const PutOperation* request, StatusResult* response) = 0;
        virtual void putAll(::grpc::ServerReaderWriter<StatusResult, PutOperation>* stream) = 0;
        virtual void remove(const KeyOperation* request, StatusResult* response) = 0;
        virtual void removeAll(::grpc::ServerReaderWriter<StatusResult, KeyOperation> *stream) = 0;

    };

    // conf_str is the json config
    Driver* create_aerospike_driver(const std::string &conf_str, const std::string &lua_path);

}

namespace gkvs_tests {

    bool as_run_tests();

}