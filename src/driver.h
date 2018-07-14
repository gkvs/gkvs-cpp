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

#pragma once

#include <string>
#include <vector>
#include "gkvs.grpc.pb.h"
#include "script.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace gkvs {

    class StatusErr final {

    public:

        StatusErr() :
                statusCode_(StatusCode::ERROR_INTERNAL),
                errorCode_(-1),
                errorMessage_("")
                {}

        bool valid_key(const Key &key) {

            if (key.viewname().empty()) {
                bad_request("empty view name");
                return false;
            }

            if (key.recordkey().empty()) {
                bad_request("empty record key");
                return false;
            }

            return true;
        }

        void bad_request(const char* errorMessage) {
            statusCode_ = StatusCode::ERROR_BAD_REQUEST;
            errorCode_ = -1;
            errorMessage_ = errorMessage;
        }

        void resource(const char* errorMessage) {
            statusCode_ = StatusCode::ERROR_RESOURCE;
            errorCode_ = -1;
            errorMessage_ = errorMessage;
        }

        void driver_error(const char* errorMessage) {
            statusCode_ = StatusCode::ERROR_DRIVER;
            errorCode_ = -1;
            errorMessage_ = errorMessage;
        }

        void to_status(Status* status) {
            status->set_code(statusCode_);
            status->set_errorcode(errorCode_);
            status->set_errormessage(errorMessage_);
        }

    private:

        StatusCode statusCode_;
        int errorCode_;
        const char* errorMessage_;

    };

    namespace status {

        inline static void success(Status *status) {
            status->set_code(StatusCode::SUCCESS);
        }

        inline static void success_not_updated(Status *status) {
            status->set_code(StatusCode::SUCCESS_NOT_UPDATED);
        }

        inline static void bad_request(int code, const char *errorMessage, Status *status) {
            status->set_code(StatusCode::ERROR_BAD_REQUEST);
            status->set_errorcode(code);
            status->set_errormessage(errorMessage);
        }

        inline static void bad_request(const char *errorMessage, Status *status) {
            bad_request(-1, errorMessage, status);
        }

        inline static void error_resource(int code, const char *errorMessage, Status *status) {
            status->set_code(StatusCode::ERROR_RESOURCE);
            status->set_errorcode(code);
            status->set_errormessage(errorMessage);
        }

        inline static void error_resource(const char *errorMessage, Status *status) {
            error_resource(-1, errorMessage, status);
        }

        inline static void unsupported(int code, const char *errorMessage, Status *status) {
            status->set_code(StatusCode::ERROR_UNSUPPORTED);
            status->set_errorcode(code);
            status->set_errormessage(errorMessage);
        }

        inline static void unsupported(const char *errorMessage, Status *status) {
            unsupported(-1, errorMessage, status);
        }

        inline static void driver_error(int code, const char *errorMessage, Status *status) {
            status->set_code(StatusCode::ERROR_DRIVER);
            status->set_errorcode(code);
            status->set_errormessage(errorMessage);
        }

        inline static void status_error(int code, const char *errorMessage, Status *status) {
            status->set_code(StatusCode::ERROR_ABORTED);
            status->set_errorcode(code);
            status->set_errormessage(errorMessage);
        }

        inline static void driver_error(const char *errorMessage, Status *status) {
            driver_error(-1, errorMessage, status);
        }

    }

    namespace check {

        inline static bool include_value(const OutputOptions &out) {

            switch(out) {
                case VALUE:
                case KEYVALUE:
                    return true;
                default:
                    break;
            }

            return false;
        }

        inline static bool include_key(const OutputOptions &out) {

            switch(out) {
                case KEY:
                case KEYVALUE:
                    return true;
                default:
                    break;
            }

            return false;

        }

    }

    namespace result {

        inline static void header(const OperationHeader& request, ResultHeader *result) {
            result->set_tag(request.tag());
        }

        inline static void elapsed(double elapsed, ResultHeader *result) {
            result->set_elapsed(elapsed);
        }

        inline static void key(const Key& key , ValueResult *result, const OutputOptions &out) {

            if (check::include_key(out)) {
                result->mutable_key()->CopyFrom(key);
            }

        }

    }

    class MultiGetEntry final {

    public:

        MultiGetEntry(const KeyOperation& request, const std::string& table, ValueResult* response)
                : request_(request), table_(table), response_(response) {}

        MultiGetEntry(const MultiGetEntry& other)
                : request_(other.request_), table_(other.table_), response_(other.response_) {}

        const KeyOperation& get_request() const {
            return request_;
        }

        const std::string& get_table() const {
            return table_;
        }

        ValueResult* get_response() const {
            return response_;
        }

    private:

        const KeyOperation& request_;
        const std::string& table_;
        ValueResult* response_;
    };

    class Driver {

    public:

        explicit Driver(const std::string& name) : name_(name) {
        }
        virtual ~Driver() = default;

        virtual bool configure(const json &conf, std::string& error) = 0;
        virtual bool connect(std::string& error) = 0;

        virtual void get(const KeyOperation* request, const std::string& table, ValueResult* response) = 0;
        virtual void multiGet(const std::vector<MultiGetEntry>& entries) = 0;
        virtual void scan(const ScanOperation* request, const std::string& table, ::grpc::ServerWriter< ValueResult>* writer) = 0;
        virtual void put(const PutOperation* request, const std::string& table, StatusResult* response) = 0;
        virtual void remove(const KeyOperation* request, const std::string& table, StatusResult* response) = 0;

        virtual bool add_table(const std::string& table, const json& conf, std::string& error) = 0;

        inline const std::string& get_name() const {
            return name_;
        }

    private:

        std::string name_;

    protected:

    };

    // conf_str is the json config
    Driver* create_aerospike_driver(const std::string& name, const std::string& lua_path);

    bool as_run_tests();

    Driver* create_redis_driver(const std::string& name);

    Driver* create_rocks_driver(const std::string& name, const std::string& work_dir);

}

