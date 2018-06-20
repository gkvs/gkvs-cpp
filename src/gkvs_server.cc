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

    class GenericStoreAsync final {

    public:

        GenericStoreAsync(std::shared_ptr<gkvs::Driver> driver,
                          std::shared_ptr<grpc::ServerCredentials> creds) {

            this->_driver = driver;
            this->_creds = creds;
        }

        ~GenericStoreAsync() {
            _server->Shutdown();
            _cq->Shutdown();
        }

        void run() {

            std::string server_address("0.0.0.0:50051");

            ServerBuilder builder;
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
            builder.RegisterService(&_service);
            _cq = builder.AddCompletionQueue();
            _server = builder.BuildAndStart();
            std::cout << "Server listening on " << server_address << std::endl;

            // Proceed to the server's main loop.
            loop();

        }

    private:

        void loop() {

        }


        gkvs::GenericStore::AsyncService _service;
        std::unique_ptr<grpc::ServerCompletionQueue> _cq;
        std::unique_ptr<Server> _server;

        std::shared_ptr<gkvs::Driver> _driver;
        std::shared_ptr<grpc::ServerCredentials> _creds;

    };

    class GenericStoreImpl final : public gkvs::GenericStore::Service {

    public:

        explicit GenericStoreImpl(std::shared_ptr<gkvs::Driver> driver) {
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
        std::shared_ptr<gkvs::Driver> _driver;



    protected:


    };


}


DEFINE_string(lua_dir, "", "User lua scripts directory for Aerospike");
DEFINE_bool(run_tests, false, "Run functional tests");
DEFINE_string(host_port, "0.0.0.0:4040", "Bind sync server host:port");

std::unique_ptr<gkvs::GenericStoreImpl> sync_service = nullptr;
std::unique_ptr<Server> sync_server = nullptr;

void onTerminate(int sign)
{
    if (sync_server != nullptr) {
        sync_server->Shutdown();
    }

}

std::shared_ptr<gkvs::Driver> create_aerospike_driver(const std::string& filename) {

    std::string aerospike_conf = gkvs::get_file_content(filename);

    gkvs::Driver *driver = gkvs::create_aerospike_driver(aerospike_conf, FLAGS_lua_dir);

    return std::shared_ptr<gkvs::Driver>(driver);
}

std::shared_ptr<grpc::ServerCredentials> create_server_credentials() {

    std::string gkvs_keys = gkvs::get_keys();
    std::string hostname = gkvs::get_hostname();

    std::cout << "Hostname: " << hostname << std::endl;

    std::string server_key = gkvs_keys + "/" + hostname + ".key";
    std::string server_crt = gkvs_keys + "/" + hostname + ".crt";
    std::string root_crt = gkvs_keys + "/GkvsAuth.crt";

    std::cout << "Use server_key: " << server_key << std::endl;
    std::cout << "Use server_crt: " << server_crt << std::endl;
    std::cout << "Use root_crt: " << root_crt << std::endl;

    grpc::SslServerCredentialsOptions::PemKeyCertPair pkcp = {
            gkvs::get_file_content(server_key),
            gkvs::get_file_content(server_crt)
    };

    grpc::SslServerCredentialsOptions ssl_options(GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE);
    ssl_options.pem_key_cert_pairs.push_back(pkcp);
    ssl_options.pem_root_certs = gkvs::get_file_content(root_crt);

    std::shared_ptr<grpc::ServerCredentials> creds = grpc::SslServerCredentials(ssl_options);

    return creds;
}

void build_sync_server(std::shared_ptr<gkvs::Driver> driver, std::shared_ptr<grpc::ServerCredentials> creds) {

    std::string server_address(FLAGS_host_port);

    sync_service = std::unique_ptr<gkvs::GenericStoreImpl> ( new gkvs::GenericStoreImpl(driver) );

    ServerBuilder builder;
    builder.AddListeningPort(server_address, creds);
    builder.RegisterService(sync_service.get());

    sync_server = builder.BuildAndStart();

    std::cout << "Server listening on " << server_address << std::endl;

}


void RunServer(const std::string& db_path, const std::string& as_filename) {

    std::shared_ptr<gkvs::Driver> driver = create_aerospike_driver(as_filename);
    std::shared_ptr<grpc::ServerCredentials> creds = create_server_credentials();

    build_sync_server(driver, creds);

    signal(SIGINT, &onTerminate);
    signal(SIGTERM, &onTerminate);


    gkvs::GenericStoreAsync async(driver, creds);
    async.run();

    sync_server->Wait();

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

    gflags::SetUsageMessage("GKVS Server)");
    gflags::SetVersionString("0.1");

    gflags::ParseCommandLineFlags(&argc, &argv,
            /*remove_flags=*/true);

    std::cout << "GKVS Server lua_dir:" <<  FLAGS_lua_dir << std::endl;

    int exitCode = 0;
    if (FLAGS_run_tests) {
        exitCode = run_tests() ? 0 : 1;
    }
    else {
        try {
            RunServer(".", "as1.json");
        }
        catch (const std::exception& e) {
            std::cout << "sync_server run exception:" << e.what() << std::endl;
            exitCode = 1;
        }
    }

    google::ShutdownGoogleLogging();
    gflags::ShutDownCommandLineFlags();

    std::cout << "GKVS Server Shutdown: " << exitCode << std::endl;

    return exitCode;
}
