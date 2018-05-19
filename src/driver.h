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

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace gkvs {

    class Driver {

    public:
        Driver() = default;
        virtual ~Driver() = default;
        virtual void getHead(const ::gkvs::KeyOperation* request, ::gkvs::HeadResult* response) = 0;
        virtual void multiGetHead(const ::gkvs::BatchKeyOperation* request, ::grpc::ServerWriter< ::gkvs::HeadResult>* writer) = 0;
        virtual void get(const ::gkvs::KeyOperation* request, ::gkvs::RecordResult* response) = 0;
        virtual void multiGet(const ::gkvs::BatchKeyOperation* request, ::grpc::ServerWriter< ::gkvs::RecordResult>* writer) = 0;
        virtual void scanHead(const ::gkvs::ScanOperation* request, ::grpc::ServerWriter< ::gkvs::HeadResult>* writer) = 0;
        virtual void scan(const ::gkvs::ScanOperation* request, ::grpc::ServerWriter< ::gkvs::RecordResult>* writer) = 0;
        virtual void put(const ::gkvs::PutOperation* request, ::gkvs::Status* response) = 0;
        virtual void compareAndPut(const ::gkvs::PutOperation* request, ::gkvs::Status* response) = 0;
        virtual void putAll(::grpc::ServerReaderWriter< ::gkvs::Status, ::gkvs::PutOperation>* stream) = 0;
        virtual void remove(const ::gkvs::KeyOperation* request, ::gkvs::Status* response) = 0;
        virtual void removeAll(const ::gkvs::BatchKeyOperation* request, ::gkvs::Status* response) = 0;

    };


    Driver* create_aerospike_driver(const json &conf, const std::string &lua_path);

}


