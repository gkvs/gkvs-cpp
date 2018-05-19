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
        Driver();
        virtual ~Driver();
        virtual void getHead(const ::gkvs::KeyOperation* request, ::gkvs::HeadResult* response);
        virtual void multiGetHead(const ::gkvs::BatchKeyOperation* request, ::grpc::ServerWriter< ::gkvs::HeadResult>* writer);
        virtual void get(const ::gkvs::KeyOperation* request, ::gkvs::RecordResult* response);
        virtual void multiGet(const ::gkvs::BatchKeyOperation* request, ::grpc::ServerWriter< ::gkvs::RecordResult>* writer);
        virtual void scanHead(const ::gkvs::ScanOperation* request, ::grpc::ServerWriter< ::gkvs::HeadResult>* writer);
        virtual void scan(const ::gkvs::ScanOperation* request, ::grpc::ServerWriter< ::gkvs::RecordResult>* writer);
        virtual void put(const ::gkvs::PutOperation* request, ::gkvs::Status* response);
        virtual void compareAndPut(const ::gkvs::PutOperation* request, ::gkvs::Status* response);
        virtual void putAll(::grpc::ServerReaderWriter< ::gkvs::Status, ::gkvs::PutOperation>* stream);
        virtual void remove(const ::gkvs::KeyOperation* request, ::gkvs::Status* response);
        virtual void removeAll(const ::gkvs::BatchKeyOperation* request, ::gkvs::Status* response);

    };


}
