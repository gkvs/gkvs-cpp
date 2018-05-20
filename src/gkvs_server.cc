/*
 *
 * Copyright 2018 gKVS authors.
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
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>

#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "helper.h"
#include "gkvs.grpc.pb.h"
#include "driver.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using std::chrono::system_clock;

namespace gkvs {


    class GenericStoreImpl final : public gkvs::GenericStore::Service {

    public:

        explicit GenericStoreImpl(gkvs::Driver *driver) {
            _driver = driver;
        }

        ~GenericStoreImpl() override {
            delete _driver;
        }

        grpc::Status getHead(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request,
                       ::gkvs::HeadResult *response) override {

            if (!request->has_key()) {
                bad_request("no key", response->mutable_status());
                return grpc::Status::OK;
            }

            if (!request->has_op()) {
                bad_request("no op", response->mutable_status());
                return grpc::Status::OK;
            }

            if (!check_key(request->key(), response->mutable_status())) {
                return grpc::Status::OK;
            }

            _driver->getHead(request, response);

            return grpc::Status::OK;

        }

        grpc::Status multiGetHead(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                            ::grpc::ServerWriter<::gkvs::HeadResult> *writer) override {

            if (!request->has_ops()) {

                HeadResult result;
                result.mutable_status()->set_code(Status_Code_SUCCESS_END_STREAM);

                writer->WriteLast(result, grpc::WriteOptions());

                return grpc::Status::OK;
            }


            _driver->multiGetHead(request, writer);

            return grpc::Status::OK;
        }

        grpc::Status get(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request,
                   ::gkvs::RecordResult *response) override {

            if (!request->has_key()) {
                bad_request("no key", response->mutable_status());
                return grpc::Status::OK;
            }

            if (!request->has_op()) {
                bad_request("no op", response->mutable_status());
                return grpc::Status::OK;
            }

            if (!check_key(request->key(), response->mutable_status())) {
                return grpc::Status::OK;
            }

            _driver->get(request, response);

            return grpc::Status::OK;
        }

        grpc::Status multiGet(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                        ::grpc::ServerWriter<::gkvs::RecordResult> *writer) override {

            if (!request->has_ops()) {

                RecordResult result;
                result.mutable_status()->set_code(Status_Code_SUCCESS_END_STREAM);

                writer->WriteLast(result, grpc::WriteOptions());

                return grpc::Status::OK;
            }

            _driver->multiGet(request, writer);

            return grpc::Status::OK;
        }

        grpc::Status scanHead(::grpc::ServerContext *context, const ::gkvs::ScanOperation *request,
                        ::grpc::ServerWriter<::gkvs::HeadResult> *writer) override {


            if (request->tablename().empty()) {

                HeadResult result;
                result.mutable_status()->set_code(Status_Code_ERROR_BAD_REQUEST);

                writer->WriteLast(result, grpc::WriteOptions());

                return grpc::Status::OK;
            }

            _driver->scanHead(request, writer);

            return grpc::Status::OK;
        }

        grpc::Status scan(::grpc::ServerContext *context, const ::gkvs::ScanOperation *request,
                    ::grpc::ServerWriter<::gkvs::RecordResult> *writer) override {

            if (request->tablename().empty()) {

                RecordResult result;
                result.mutable_status()->set_code(Status_Code_ERROR_BAD_REQUEST);

                writer->WriteLast(result, grpc::WriteOptions());

                return grpc::Status::OK;
            }

            _driver->scan(request, writer);

            return grpc::Status::OK;
        }

        grpc::Status
        put(::grpc::ServerContext *context, const ::gkvs::PutOperation *request, ::gkvs::Status *response) override {

            if (!request->has_key()) {
                bad_request("no key", response);
                return grpc::Status::OK;
            }

            if (!request->has_record()) {
                bad_request("no record", response);
                return grpc::Status::OK;
            }

            if (!request->has_op()) {
                bad_request("no op", response);
                return grpc::Status::OK;
            }

            if (!check_key(request->key(), response)) {
                return grpc::Status::OK;
            }

            _driver->put(request, response);

            return grpc::Status::OK;
        }

        grpc::Status compareAndPut(::grpc::ServerContext *context, const ::gkvs::PutOperation *request,
                             ::gkvs::Status *response) override {

            if (!request->has_key()) {
                bad_request("no key", response);
                return grpc::Status::OK;
            }

            if (!request->has_record()) {
                bad_request("no record", response);
                return grpc::Status::OK;
            }

            if (!request->has_op()) {
                bad_request("no op", response);
                return grpc::Status::OK;
            }

            if (!check_key(request->key(), response)) {
                return grpc::Status::OK;
            }

            _driver->compareAndPut(request, response);

            return grpc::Status::OK;
        }

        grpc::Status putAll(::grpc::ServerContext *context,
                      ::grpc::ServerReaderWriter<::gkvs::Status, ::gkvs::PutOperation> *stream) override {

            _driver->putAll(stream);

            return grpc::Status::OK;
        }

        grpc::Status
        remove(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request, ::gkvs::Status *response) override {

            if (!request->has_key()) {
                bad_request("no key", response);
                return grpc::Status::OK;
            }

            if (!request->has_op()) {
                bad_request("no op", response);
                return grpc::Status::OK;
            }

            if (!check_key(request->key(), response)) {
                return grpc::Status::OK;
            }

            _driver->remove(request, response);

            return grpc::Status::OK;
        }

        grpc::Status removeAll(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                         ::gkvs::Status *response) override {

            _driver->removeAll(request, response);

            return grpc::Status::OK;
        }

    private:
        gkvs::Driver *_driver;



    protected:


        bool check_key(const ::gkvs::Key &key, Status *status) {

            if (key.tablename().empty()) {
                bad_request("empty tableName", status);
                return false;
            }

            if (key.recordRef_case() == Key::RecordRefCase::kRecordKey) {
                if (key.recordkey().empty()) {
                    bad_request("empty recordKey", status);
                    return false;
                }
            }

            return true;
        }

        void bad_request(const char *errorMessage, Status *status) {
            status->set_code(Status_Code_ERROR_BAD_REQUEST);
            status->set_errorcode(Status_Code_ERROR_BAD_REQUEST);
            status->set_errormessage(errorMessage);
        }


    };


}

DEFINE_string(lua_dir, "", "User lua scripts directory for Aerospike");


void RunServer(const std::string& db_path) {


    json aerospike_conf = R"({

    "namespace": "test",

    "cluster": {
         "host" : "192.168.56.101",
         "port" : 3000,
         "username" : "",
         "password" : ""
     }

    })"_json;


  gkvs::Driver *driver = gkvs::create_aerospike_driver(aerospike_conf, FLAGS_lua_dir);

  std::string server_address("0.0.0.0:4040");
  gkvs::GenericStoreImpl service(driver);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}


int main(int argc, char** argv) {

    google::InitGoogleLogging(argv[0]);

    gflags::SetUsageMessage("gKVS Server)");
    gflags::SetVersionString("0.1");

    gflags::ParseCommandLineFlags(&argc, &argv,
            /*remove_flags=*/true);

    std::cout << "gKVS Server lua_dir:" <<  FLAGS_lua_dir << std::endl;

    RunServer(".");

    google::ShutdownGoogleLogging();
    gflags::ShutDownCommandLineFlags();

    return 0;
}
