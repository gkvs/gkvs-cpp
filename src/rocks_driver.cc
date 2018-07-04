/*
 *
 * Copyright 2018-present GKVS authors.
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

#include "driver.h"
#include "rocks_driver.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace gkvs {


    class RocksDriver final : public Driver {

    public:

        explicit RocksDriver(const std::string& name, const json &conf, const std::string& db_dir) : Driver(name) {



        }

        ~RocksDriver() override {

            std::cout << "Graceful shutdown rocks driver" << std::endl;

        }


        void get(const KeyOperation *request, ValueResult *response) override {

            do_get(request, response);

        }

        void multiGet(const BatchKeyOperation *request, BatchValueResult *response) override {

            do_multi_get(request, response);

        }

        void scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer) override {

            do_scan(request, writer);

        }

        void put(const PutOperation *request, StatusResult *response) override {

            do_put(request, response);

        }

        void remove(const KeyOperation *request, StatusResult *response) override {

            do_remove(request, response);

        }


    private:



    protected:

        void do_multi_get(const BatchKeyOperation *request, BatchValueResult *response);

        void do_scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer);

        void do_get(const KeyOperation *request, ValueResult *response);

        void do_put(const PutOperation *request, StatusResult *response);

        void do_remove(const KeyOperation *request, StatusResult *response);


    };


    Driver* create_rocks_driver(const std::string &name, const json& conf, const std::string &db_dir) {
        return new RocksDriver(name, conf, db_dir);
    }

}


void gkvs::RocksDriver::do_multi_get(const BatchKeyOperation *request, BatchValueResult *response) {



}

void gkvs::RocksDriver::do_scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer) {


}

void gkvs::RocksDriver::do_get(const KeyOperation *request, ValueResult *response) {

    success(response->mutable_status());

}

void gkvs::RocksDriver::do_put(const PutOperation *request, StatusResult *response) {

    success(response->mutable_status());

}

void gkvs::RocksDriver::do_remove(const KeyOperation *request, StatusResult *response) {

    success(response->mutable_status());

}