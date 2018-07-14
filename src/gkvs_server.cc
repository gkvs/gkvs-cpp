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
#include <unordered_map>

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
#include "script.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using std::chrono::system_clock;

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace gkvs {

    class Redirect {

    public:

        Redirect(const std::string& view, const std::string& cluster, Driver* driver, const std::string& table)
                : view_(view), cluster_(cluster), driver_(driver), table_(table) {
        }

        const std::string& get_view() const {
            return view_;
        }

        const std::string& get_cluster() const {
            return cluster_;
        }

        Driver* get_driver() const {
            return driver_;
        }

        const std::string& get_table() const {
            return table_;
        }

    private:

        std::string view_;
        std::string cluster_;
        Driver* driver_;
        std::string table_;

    };


    class GenericStoreImpl final : public gkvs::GenericStore::Service {

    public:

        explicit GenericStoreImpl() {
        }

        ~GenericStoreImpl() {

            for (auto i = drivers_.begin(); i != drivers_.end(); ++i) {
                Driver* driver = i->second;
                delete driver;
            }

            for (auto i = views_.begin(); i != views_.end(); ++i) {
                Redirect* redirect = i->second;
                delete redirect;
            }
        }

        bool add_driver(const std::string& cluster, Driver* driver, std::string& error) {

            if (cluster.empty()) {
                error = "empty cluster name";
                return false;
            }

            auto i = drivers_.find(cluster);

            if (i != drivers_.end()) {
                error = "cluster already exists: " + cluster;
                return false;
            }

            drivers_[cluster] = driver;

            return true;
        }

        bool add_table(const std::string& table, const std::string& cluster, const json& conf, std::string& error) {

            if (table.empty()) {
                error = "empty table name";
                return false;
            }

            if (cluster.empty()) {
                error = "empty cluster name";
                return false;
            }

            auto i = drivers_.find(cluster);

            if (i == drivers_.end()) {
                error = "cluster not found: " + cluster;
                return false;
            }

            return i->second->add_table(table, conf, error);

        }

        bool add_view(const std::string& view, const std::string& cluster, const std::string& table, std::string& error) {

            if (view.empty()) {
                error = "empty view name";
                return false;
            }

            if (cluster.empty()) {
                error = "empty cluster name";
                return false;
            }

            if (table.empty()) {
                error = "empty table name";
                return false;
            }

            auto i = views_.find(view);
            if (i != views_.end()) {
                error = "view already exists: " + view;
                return false;
            }

            auto c = drivers_.find(cluster);
            if (c == drivers_.end()) {
                error = "cluster not found: " + cluster;
                return false;
            }

            Driver* driver = c->second;

            views_[view] = new Redirect(view, cluster, driver, table);

            return true;

        }

        grpc::Status get(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request,
                         ::gkvs::ValueResult *response) override {

            auto begin = std::chrono::high_resolution_clock::now();

            grpc::Status status = do_get(context, request, response);

            auto end = std::chrono::high_resolution_clock::now();
            double elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() / 1000000;

            result::elapsed(elapsed, response->mutable_header());

            return status;
        }

        grpc::Status do_get(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request,
                   ::gkvs::ValueResult *response) {

            // for client identification purpose
            result::header(request->header(), response->mutable_header());

            if (!request->has_key()) {
                status::bad_request("no key", response->mutable_status());
                return grpc::Status::OK;
            }

            // for client identification purpose
            result::key(request->key(), response, request->output());

            StatusErr statusErr;
            if (!statusErr.valid_key(request->key())) {
                statusErr.to_status(response->mutable_status());
                return grpc::Status::OK;
            }

            const std::string& view = request->key().viewname();

            auto i = views_.find(view);
            if (i == views_.end()) {
                status::error_resource("view not found", response->mutable_status());
                return grpc::Status::OK;
            }

            Redirect* redirect = i->second;

            Driver* driver = redirect->get_driver();
            const std::string& table = redirect->get_table();

            driver->get(request, table, response);

            return grpc::Status::OK;
        }

        grpc::Status multiGet(::grpc::ServerContext *context, const ::gkvs::BatchKeyOperation *request,
                              ::gkvs::BatchValueResult *response) override {

            std::unordered_map<Driver*, std::vector<MultiGetEntry>> map;

            int size = request->operation_size();
            for (int i = 0; i < size; ++i) {

                const KeyOperation& op = request->operation(i);
                ValueResult *result = response->add_result();

                // for client identification purpose
                result::header(op.header(), result->mutable_header());

                if (!op.has_key()) {
                    status::bad_request("no key", result->mutable_status());
                    continue;
                }

                // for client identification purpose
                result::key(op.key(), result, op.output());

                StatusErr statusErr;
                if (!statusErr.valid_key(op.key())) {
                    statusErr.to_status(result->mutable_status());
                    continue;
                }

                const std::string& view = op.key().viewname();

                auto v = views_.find(view);
                if (v == views_.end()) {
                    status::error_resource("view not found", result->mutable_status());
                    continue;
                }

                Redirect* redirect = v->second;

                Driver* driver = redirect->get_driver();
                const std::string& table = redirect->get_table();

                if (map.find(driver) == map.end()) {
                    map[driver] = std::vector<MultiGetEntry>();
                }

                map[driver].push_back(MultiGetEntry(op, table, result));
            }

            // run

            for (auto k = map.begin(); k != map.end(); ++k) {

                Driver* driver = k->first;
                std::vector<MultiGetEntry>& entries = k->second;

                auto begin = std::chrono::high_resolution_clock::now();

                driver->multiGet(entries);

                auto end = std::chrono::high_resolution_clock::now();
                double elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() / 1000000;

                for (auto e = entries.begin(); e != entries.end(); ++e) {
                    result::elapsed(elapsed, e->get_response()->mutable_header());
                }

            }

            return grpc::Status::OK;
        }

        grpc::Status getAll(::grpc::ServerContext *context,
                            ::grpc::ServerReaderWriter<::gkvs::ValueResult, ::gkvs::KeyOperation> *stream) override {


            KeyOperation request;
            ValueResult response;

            while (stream->Read(&request)) {

                response.Clear();

                get(context, &request, &response);

                stream->Write(response);

            }

            return grpc::Status::OK;

        }

        grpc::Status scan(::grpc::ServerContext *context, const ::gkvs::ScanOperation *request,
                    ::grpc::ServerWriter<::gkvs::ValueResult> *writer) override {

            const std::string& view = request->viewname();

            if (view.empty()) {
                ValueResult result;
                result::header(request->header(), result.mutable_header());
                status::bad_request("empty view name", result.mutable_status());
                writer->WriteLast(result, grpc::WriteOptions());
                return grpc::Status::OK;
            }

            auto i = views_.find(view);

            if (i == views_.end()) {
                ValueResult result;
                result::header(request->header(), result.mutable_header());
                status::error_resource("view not found", result.mutable_status());
                writer->WriteLast(result, grpc::WriteOptions());
                return grpc::Status::OK;
            }

            Redirect* redirect = i->second;
            Driver* driver = redirect->get_driver();
            const std::string& table = redirect->get_table();

            driver->scan(request, table, writer);

            return grpc::Status::OK;
        }

        grpc::Status
        put(::grpc::ServerContext *context, const ::gkvs::PutOperation *request, ::gkvs::StatusResult *response) override {

            auto begin = std::chrono::high_resolution_clock::now();

            grpc::Status status = do_put(context, request, response);

            auto end = std::chrono::high_resolution_clock::now();
            double elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() / 1000000;

            result::elapsed(elapsed, response->mutable_header());

            return status;

        }

        grpc::Status
        do_put(::grpc::ServerContext *context, const ::gkvs::PutOperation *request, ::gkvs::StatusResult *response) {

            // for client identification purpose
            result::header(request->header(), response->mutable_header());

            if (!request->has_key()) {
                status::bad_request("no key", response->mutable_status());
                return grpc::Status::OK;
            }

            StatusErr statusErr;
            if (!statusErr.valid_key(request->key())) {
                statusErr.to_status(response->mutable_status());
                return grpc::Status::OK;
            }

            const std::string& view = request->key().viewname();

            auto i = views_.find(view);

            if (i == views_.end()) {
                status::error_resource("view not found", response->mutable_status());
                return grpc::Status::OK;
            }

            Redirect* redirect = i->second;

            Driver* driver = redirect->get_driver();
            const std::string& table = redirect->get_table();

            driver->put(request, table, response);

            return grpc::Status::OK;
        }


        grpc::Status putAll(::grpc::ServerContext *context,
                      ::grpc::ServerReaderWriter<::gkvs::StatusResult, ::gkvs::PutOperation> *stream) override {

            PutOperation request;
            StatusResult response;

            while (stream->Read(&request)) {

                response.Clear();

                put(context, &request, &response);

                stream->Write(response);

            }


            return grpc::Status::OK;
        }

        grpc::Status
        remove(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request, ::gkvs::StatusResult *response) override {

            auto begin = std::chrono::high_resolution_clock::now();

            grpc::Status status = do_remove(context, request, response);

            auto end = std::chrono::high_resolution_clock::now();
            double elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() / 1000000;

            result::elapsed(elapsed, response->mutable_header());

            return status;

        }

        grpc::Status
        do_remove(::grpc::ServerContext *context, const ::gkvs::KeyOperation *request, ::gkvs::StatusResult *response) {

            // for client identification purpose
            result::header(request->header(), response->mutable_header());

            if (!request->has_key()) {
                status::bad_request("no key", response->mutable_status());
                return grpc::Status::OK;
            }

            StatusErr statusErr;
            if (!statusErr.valid_key(request->key())) {
                statusErr.to_status(response->mutable_status());
                return grpc::Status::OK;
            }

            const std::string& view = request->key().viewname();

            auto i = views_.find(view);

            if (i == views_.end()) {
               status::error_resource("view not found", response->mutable_status());
                return grpc::Status::OK;
            }

            Redirect* redirect = i->second;

            Driver* driver = redirect->get_driver();
            const std::string& table = redirect->get_table();

            driver->remove(request, table, response);

            return grpc::Status::OK;
        }

        grpc::Status removeAll(::grpc::ServerContext *context,
                               ::grpc::ServerReaderWriter<::gkvs::StatusResult, ::gkvs::KeyOperation> *stream) override {

            KeyOperation request;
            StatusResult response;

            while (stream->Read(&request)) {

                response.Clear();

                remove(context, &request, &response);

                stream->Write(response);

            }

            return grpc::Status::OK;

        }


    private:

        std::unordered_map<std::string, Driver*> drivers_;
        std::unordered_map<std::string, Redirect*> views_;


    protected:

    };


}

DEFINE_string(work_dir, ".", "Work dir");
DEFINE_string(lua_dir, "", "User lua scripts directory for Aerospike");
DEFINE_bool(run_tests, false, "Run functional tests");
DEFINE_string(host_port, "0.0.0.0:4040", "Bind sync server host:port");

std::unique_ptr<gkvs::GenericStoreImpl> sync_service ( new gkvs::GenericStoreImpl() );
std::unique_ptr<Server> sync_server = nullptr;

void onTerminate(int sign)
{
    if (sync_server != nullptr) {
        sync_server->Shutdown();
    }

}

bool gkvs::add_cluster(const std::string& cluster, const std::string& driver, const std::string& msgpack, std::string& error) {

    gkvs::Driver* dr;

    int size = msgpack.size();
    std::vector<uint8_t> input;
    for (int i = 0; i < size; ++i) {
        char ch = msgpack[i];
        uint8_t uch = static_cast<uint8_t>(ch);
        input.push_back(uch);
    }

    json conf = json::from_msgpack(input);

    if (driver == "redis") {

        dr = gkvs::create_redis_driver(cluster);

    }
    else if (driver == "aerospike") {

        dr = gkvs::create_aerospike_driver(cluster, FLAGS_lua_dir);

    }
    else if (driver == "rocks") {

        dr = gkvs::create_rocks_driver(cluster, FLAGS_work_dir);

    }
    else {
        error = "unknown driver: " + driver;
        return false;
    }

    if (!dr->configure(conf, error)) {
        return false;
    }

    if (!dr->connect(error)) {
        return false;
    }

    return sync_service->add_driver(cluster, dr, error);

}

bool gkvs::add_table(const std::string& table, const std::string& cluster, const std::string& msgpack, std::string& error) {

    int size = msgpack.size();
    std::vector<uint8_t> input;
    for (int i = 0; i < size; ++i) {
        char ch = msgpack[i];
        uint8_t uch = static_cast<uint8_t>(ch);
        input.push_back(uch);
    }

    json conf = json::from_msgpack(input);

    return sync_service->add_table(table, cluster, conf, error);
}

bool gkvs::add_view(const std::string& view, const std::string& cluster, const std::string& table, std::string& error) {
    return sync_service->add_view(view, cluster, table, error);
}


bool load_script(const std::string& content) {

    std::string error;
    gkvs::lua_script script;
    if (!script.loadstring(content, error)) {
        LOG(ERROR) << "Lua script error: " << error << std::endl;
        return false;
    }

    return true;
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

void build_sync_server(std::shared_ptr<grpc::ServerCredentials> creds) {

    std::string server_address(FLAGS_host_port);

    ServerBuilder builder;
    builder.AddListeningPort(server_address, creds);
    builder.RegisterService(sync_service.get());

    sync_server = builder.BuildAndStart();

    std::cout << "Server listening on " << server_address << std::endl;

}


void RunServer(const std::string& filename) {

    std::string content = gkvs::get_file_content(filename);

    if (!load_script(content)) {
        LOG(ERROR) << "failed to parse config: " << content << std::endl;
        return;
    }

    std::shared_ptr<grpc::ServerCredentials> creds = create_server_credentials();

    build_sync_server(creds);

    signal(SIGINT, &onTerminate);
    signal(SIGTERM, &onTerminate);

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

        if (argc < 2) {
            std::cout << "Usage: gkvs_server aerospike_config.json" << std::endl;
            exitCode = 1;
        }
        else {

            try {

                RunServer(argv[1]);
            }
            catch (const std::exception &e) {
                std::cout << "sync_server run exception:" << e.what() << std::endl;
                exitCode = 1;
            }

        }
    }

    google::ShutdownGoogleLogging();
    gflags::ShutDownCommandLineFlags();

    std::cout << "GKVS Server Shutdown: " << exitCode << std::endl;

    return exitCode;
}
