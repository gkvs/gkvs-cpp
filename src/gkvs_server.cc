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
#include "helper.h"
#include "gkvs.grpc.pb.h"

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

    explicit GenericStoreImpl(const std::string &db) {

    }


    Status getHead(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request,
                   ::gkvs::HeadResult *response) override {
      return Service::getHead(context, request, response);
    }

    Status multiGetHead(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                        ::grpc::ServerWriter<::gkvs::HeadResult> *writer) override {
      return Service::multiGetHead(context, request, writer);
    }

    Status
    get(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request, ::gkvs::RecordResult *response) override {
      return Service::get(context, request, response);
    }

    Status multiGet(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                    ::grpc::ServerWriter<::gkvs::RecordResult> *writer) override {
      return Service::multiGet(context, request, writer);
    }

    Status scanHead(::grpc::ServerContext *context, const ::gkvs::ScanOperation *request,
                    ::grpc::ServerWriter<::gkvs::HeadResult> *writer) override {
      return Service::scanHead(context, request, writer);
    }

    Status scan(::grpc::ServerContext *context, const ::gkvs::ScanOperation *request,
                ::grpc::ServerWriter<::gkvs::RecordResult> *writer) override {
      return Service::scan(context, request, writer);
    }

    Status put(::grpc::ServerContext *context, const ::gkvs::PutOperation *request, ::gkvs::Status *response) override {
      return Service::put(context, request, response);
    }

    Status compareAndPut(::grpc::ServerContext *context, const ::gkvs::PutOperation *request,
                         ::gkvs::Status *response) override {
      return Service::compareAndPut(context, request, response);
    }

    Status putAll(::grpc::ServerContext *context,
                  ::grpc::ServerReaderWriter<::gkvs::Status, ::gkvs::PutOperation> *stream) override {
      return Service::putAll(context, stream);
    }

    Status
    remove(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request, ::gkvs::Status *response) override {
      return Service::remove(context, request, response);
    }

    Status removeAll(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                     ::gkvs::Status *response) override {
      return Service::removeAll(context, request, response);
    }

};


void RunServer(const std::string& db_path) {
  std::string server_address("0.0.0.0:4040");
  GenericStoreImpl service(db_path);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}

int main(int argc, char** argv) {

  std::cout << "GKVS Server" << std::endl;

  RunServer(".");

  return 0;
}
