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


class GenericStoreImpl final : public gkvs::GenericStore::Service {

public:

    explicit GenericStoreImpl(gkvs::Driver *driver) {
        _driver = driver;
    }

    ~GenericStoreImpl() override {
        delete _driver;
    }

    Status getHead(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request,
                   ::gkvs::HeadResult *response) override {


        _driver->getHead(request, response);

        response->mutable_status()->set_code(::gkvs::Status_Code::Status_Code_SUCCESS);

        return Status::OK;

    }

    Status multiGetHead(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                        ::grpc::ServerWriter<::gkvs::HeadResult> *writer) override {

        _driver->multiGetHead(request, writer);

        return Status::OK;
    }

    Status get(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request, ::gkvs::RecordResult *response) override {

        _driver->get(request, response);

        response->mutable_status()->set_code(::gkvs::Status_Code::Status_Code_SUCCESS);

        return Status::OK;
    }

    Status multiGet(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                    ::grpc::ServerWriter<::gkvs::RecordResult> *writer) override {

        _driver->multiGet(request, writer);

        return Status::OK;
    }

    Status scanHead(::grpc::ServerContext *context, const ::gkvs::ScanOperation *request,
                    ::grpc::ServerWriter<::gkvs::HeadResult> *writer) override {

        _driver->scanHead(request, writer);

        return Status::OK;
    }

    Status scan(::grpc::ServerContext *context, const ::gkvs::ScanOperation *request,
                ::grpc::ServerWriter<::gkvs::RecordResult> *writer) override {

        _driver->scan(request, writer);

        return Status::OK;
    }

    Status put(::grpc::ServerContext *context, const ::gkvs::PutOperation *request, ::gkvs::Status *response) override {

        _driver->put(request, response);

        response->set_code(::gkvs::Status_Code::Status_Code_SUCCESS);

        return Status::OK;
    }

    Status compareAndPut(::grpc::ServerContext *context, const ::gkvs::PutOperation *request,
                         ::gkvs::Status *response) override {

        _driver->compareAndPut(request, response);

        response->set_code(::gkvs::Status_Code::Status_Code_SUCCESS);

        return Status::OK;
    }

    Status putAll(::grpc::ServerContext *context,
                  ::grpc::ServerReaderWriter<::gkvs::Status, ::gkvs::PutOperation> *stream) override {

        _driver->putAll(stream);

        return Status::OK;
    }

    Status remove(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request, ::gkvs::Status *response) override {

        _driver->remove(request, response);

        response->set_code(::gkvs::Status_Code::Status_Code_SUCCESS);

        return Status::OK;
    }

    Status removeAll(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                     ::gkvs::Status *response) override {

        _driver->removeAll(request, response);

        response->set_code(::gkvs::Status_Code::Status_Code_SUCCESS);

        return Status::OK;
    }

private:
    gkvs::Driver *_driver;

};


void RunServer(const std::string& db_path) {


    json aerospike_conf = R"({

    "cluster": {
         "host" : "192.168.56.101",
         "port" : 3000,
         "username" : "",
         "password" : ""
     }

    })"_json;


  gkvs::Driver *driver = gkvs::create_aerospike_driver(aerospike_conf, ".");

  std::string server_address("0.0.0.0:4040");
  GenericStoreImpl service(driver);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}

int main(int argc, char** argv) {

    google::InitGoogleLogging(argv[0]);

    gflags::SetUsageMessage("GKVS Server)");
    gflags::SetVersionString("0.1");

    gflags::ParseCommandLineFlags(&argc, &argv,
            /*remove_flags=*/true);

    RunServer(".");

    google::ShutdownGoogleLogging();
    gflags::ShutDownCommandLineFlags();

    return 0;
}
