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

        explicit GenericStoreImpl(gkvs::Driver* driver) {
            _driver = driver;
        }


        grpc::Status get(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request,
                   ::gkvs::ValueResult *response) override {

            _driver->get(request, response);

            return grpc::Status::OK;
        }

        grpc::Status multiGet(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                              ::gkvs::BatchValueResult *response) override {

            if (!request->operation().empty()) {

                _driver->multiGet(request, response);
            }

            return grpc::Status::OK;
        }

        grpc::Status getAll(::grpc::ServerContext *context,
                            ::grpc::ServerReaderWriter<::gkvs::ValueResult, ::gkvs::KeyOperation> *stream) override {

            KeyOperation request;
            ValueResult response;

            while (stream->Read(&request)) {

                response.Clear();

                _driver->get(&request, &response);

                stream->Write(response);

            }

            return grpc::Status::OK;

        }

        grpc::Status scan(::grpc::ServerContext *context, const ::gkvs::ScanOperation *request,
                    ::grpc::ServerWriter<::gkvs::ValueResult> *writer) override {


            _driver->scan(request, writer);

            return grpc::Status::OK;
        }

        grpc::Status
        put(::grpc::ServerContext *context, const ::gkvs::PutOperation *request, ::gkvs::StatusResult *response) override {

            _driver->put(request, response);

            return grpc::Status::OK;
        }


        grpc::Status putAll(::grpc::ServerContext *context,
                      ::grpc::ServerReaderWriter<::gkvs::StatusResult, ::gkvs::PutOperation> *stream) override {


            PutOperation request;
            StatusResult response;

            while (stream->Read(&request)) {

                response.Clear();

                _driver->put(&request, &response);

                stream->Write(response);

            }


            return grpc::Status::OK;
        }

        grpc::Status
        remove(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request, ::gkvs::StatusResult *response) override {

            _driver->remove(request, response);

            return grpc::Status::OK;
        }

        grpc::Status removeAll(::grpc::ServerContext *context,
                               ::grpc::ServerReaderWriter<::gkvs::StatusResult, ::gkvs::KeyOperation> *stream) override {

            KeyOperation request;
            StatusResult response;

            while (stream->Read(&request)) {

                response.Clear();

                _driver->remove(&request, &response);

                stream->Write(response);

            }

            return grpc::Status::OK;

        }


    private:
        gkvs::Driver* _driver;



    protected:


    };


}

DEFINE_string(lua_dir, "", "User lua scripts directory for Aerospike");
DEFINE_bool(run_tests, false, "Run functional tests");
DEFINE_string(host_port, "0.0.0.0:4040", "Bind server host:port");

void RunServer(const std::string& db_path) {


    std::string aerospike_conf = R"({

    "namespace": "test",

    "cluster": {
         "host" : "192.168.56.101",
         "port" : 3000,
         "username" : "",
         "password" : ""
     }

    })";


  gkvs::Driver *driver = gkvs::create_aerospike_driver(aerospike_conf, FLAGS_lua_dir);

  std::string server_address(FLAGS_host_port);
  gkvs::GenericStoreImpl service(driver);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}


bool run_tests() {

    bool passed = true;

    passed &= gkvs::as_run_tests();

    if (passed) {
        std::cout << "SUCCESS" << std::endl;
    }
    else {
        std::cout << "FAILURE" << std::endl;
    }

    return passed;

}

int main(int argc, char** argv) {

    google::InitGoogleLogging(argv[0]);

    gflags::SetUsageMessage("gKVS Server)");
    gflags::SetVersionString("0.1");

    gflags::ParseCommandLineFlags(&argc, &argv,
            /*remove_flags=*/true);

    std::cout << "gKVS Server lua_dir:" <<  FLAGS_lua_dir << std::endl;

    int exitCode = 0;
    if (FLAGS_run_tests) {
        exitCode = run_tests() ? 0 : 1;
    }
    else {
        try {
            RunServer(".");
        }
        catch (const std::exception& e) {
            std::cout << "server run exception:" << e.what() << std::endl;
            exitCode = 1;
        }
    }

    google::ShutdownGoogleLogging();
    gflags::ShutDownCommandLineFlags();

    return exitCode;
}
