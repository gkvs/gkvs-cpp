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

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace gkvs {

    class StatusErr final {

    public:

        void bad_request(const char* errorMessage) {
            _statusCode = StatusCode::ERROR_BAD_REQUEST;
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

    class Driver {

    public:
        explicit Driver(const std::string& name) : name_(name) {
        }
        virtual ~Driver() = default;
        virtual void get(const KeyOperation* request, ValueResult* response) = 0;
        virtual void multiGet(const BatchKeyOperation *request, BatchValueResult *response) = 0;
        virtual void scan(const ScanOperation* request, ::grpc::ServerWriter< ValueResult>* writer) = 0;
        virtual void put(const PutOperation* request, StatusResult* response) = 0;
        virtual void remove(const KeyOperation* request, StatusResult* response) = 0;

        inline const std::string& get_name() {
            return name_;
        }

    private:

        std::string name_;

    protected:

        void success(Status *status) {
            status->set_code(StatusCode::SUCCESS);
        }

        void success_not_updated(Status *status) {
            status->set_code(StatusCode::SUCCESS_NOT_UPDATED);
        }

        void bad_request(int code, const char* errorMessage, Status *status) {
            status->set_code(StatusCode::ERROR_BAD_REQUEST);
            status->set_errorcode(code);
            status->set_errormessage(errorMessage);
        }

        void bad_request(const char* errorMessage, Status *status) {
            bad_request(-1, errorMessage, status);
        }

        void unsupported(int code, const char* errorMessage, Status *status) {
            status->set_code(StatusCode::ERROR_UNSUPPORTED);
            status->set_errorcode(code);
            status->set_errormessage(errorMessage);
        }

        void unsupported(const char* errorMessage, Status *status) {
            unsupported(-1, errorMessage, status);
        }

        void driver_error(int code, const char* errorMessage, Status *status) {
            status->set_code(StatusCode::ERROR_DRIVER);
            status->set_errorcode(code);
            status->set_errormessage(errorMessage);
        }

        void status_error(int code, const char* errorMessage, Status *status) {
            status->set_code(StatusCode::ERROR_ABORTED);
            status->set_errorcode(code);
            status->set_errormessage(errorMessage);
        }

        void driver_error(const char* errorMessage, Status *status) {
            driver_error(-1, errorMessage, status);
        }

        bool valid_key(const Key &key, StatusErr& statusErr) {

            if (key.tablename().empty()) {
                statusErr.bad_request("empty store name");
                return false;
            }

            switch(key.recordKey_case()) {

                case Key::RecordKeyCase::kRaw:
                    if (key.raw().empty()) {
                        statusErr.bad_request("empty record key raw");
                        return false;
                    }
                    break;

                case Key::RecordKeyCase::kDigest:
                    if (key.digest().empty()) {
                        statusErr.bad_request("empty record key digest");
                        return false;
                    }
                    break;

                default:
                    statusErr.bad_request("invalid record key type");
                    return false;
            }

            return true;
        }

        inline bool include_value(const OutputOptions &out) {

            switch(out) {
                case VALUE:
                case KEYVALUE:
                    return true;
                default:
                    break;
            }

            return false;
        }

        inline bool include_key(const OutputOptions &out) {

            switch(out) {
                case KEY:
                case KEYVALUE:
                    return true;
                default:
                    break;
            }

            return false;

        }

    };


    // conf_str is the json config
    Driver* create_aerospike_driver(const std::string& name, const json& conf, const std::string& lua_path);

    bool as_run_tests();

    Driver* create_redis_driver(const std::string& name, const json& conf);

    Driver* create_rocks_driver(const std::string& name, const json& conf, const std::string& work_dir);

}

