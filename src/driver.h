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

        bool valid_key(const Key &key) {

            if (key.tablename().empty()) {
                bad_request("empty table name");
                return false;
            }

            switch(key.recordKey_case()) {

                case Key::RecordKeyCase::kRaw:
                    if (key.raw().empty()) {
                        bad_request("empty record key raw");
                        return false;
                    }
                    break;

                case Key::RecordKeyCase::kDigest:
                    if (key.digest().empty()) {
                        bad_request("empty record key digest");
                        return false;
                    }
                    break;

                default:
                    bad_request("invalid record key type");
                    return false;
            }

            return true;
        }

        void bad_request(const char* errorMessage) {
            _statusCode = StatusCode::ERROR_BAD_REQUEST;
            _errorCode = -1;
            _errorMessage = errorMessage;
        }

        void resource(const char* errorMessage) {
            _statusCode = StatusCode::ERROR_RESOURCE;
            _errorCode = -1;
            _errorMessage = errorMessage;
        }

        void driver_error(const char* errorMessage) {
            _statusCode = StatusCode::ERROR_DRIVER;
            _errorCode = -1;
            _errorMessage = errorMessage;
        }

        void to_status(Status* status) {
            status->set_code(_statusCode);
            status->set_errorcode(_errorCode);
            status->set_errormessage(_errorMessage);
        }

    private:

        StatusCode _statusCode = StatusCode::ERROR_INTERNAL;
        int _errorCode = -1;
        const char* _errorMessage = "";

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
            result->set_elapse(elapsed);
        }

        inline static void key(const Key& key , ValueResult *result, const OutputOptions &out) {

            if (check::include_key(out)) {
                result->mutable_key()->CopyFrom(key);
            }

        }

    }

    class MultiGetEntry final {

    public:

        MultiGetEntry(const KeyOperation& request, const std::string& table_override, ValueResult* response)
                : request_(request), table_override_(table_override), response_(response) {}

        MultiGetEntry(const MultiGetEntry& other)
                : request_(other.request_), table_override_(other.table_override_), response_(other.response_) {}

        const KeyOperation& get_request() const {
            return request_;
        }

        const std::string& get_table_override() const {
            return table_override_;
        }

        ValueResult* get_response() const {
            return response_;
        }

    private:

        const KeyOperation& request_;
        const std::string& table_override_;
        ValueResult* response_;
    };

    class Driver {

    public:

        explicit Driver(const std::string& name) : name_(name) {
        }
        virtual ~Driver() = default;

        virtual void get(const KeyOperation* request, const std::string& table_override, ValueResult* response) = 0;
        virtual void multiGet(const std::vector<MultiGetEntry>& entries) = 0;
        virtual void scan(const ScanOperation* request, const std::string& table_override, ::grpc::ServerWriter< ValueResult>* writer) = 0;
        virtual void put(const PutOperation* request, const std::string& table_override, StatusResult* response) = 0;
        virtual void remove(const KeyOperation* request, const std::string& table_override, StatusResult* response) = 0;

        virtual bool add_table(const std::string& table, const json& conf, std::string& error) = 0;

        inline const std::string& get_name() const {
            return name_;
        }

    private:

        std::string name_;

    protected:

    };

    // conf_str is the json config
    Driver* create_aerospike_driver(const std::string& name, const json& conf, const std::string& lua_path);

    bool as_run_tests();

    Driver* create_redis_driver(const std::string& name, const json& conf);

    Driver* create_rocks_driver(const std::string& name, const json& conf, const std::string& work_dir);

}

