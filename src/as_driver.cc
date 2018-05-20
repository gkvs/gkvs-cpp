/*
 *
 * Copyright 2018 gGKVS authors.
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
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>

#include <glog/logging.h>

#include "driver.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;


#define AS_MAX_LOG_STR 1024
static bool glog_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...);

static gkvs::Status_Code parse_aerospike_status(as_status status);

namespace gkvs {


    void bad_request(int errorCode, std::string errorMessage, ::gkvs::HeadResult *response) {
        Status *status = response->mutable_status();
        status->set_code(Status_Code_ERROR_BAD_REQUEST);
        status->set_errorcode(errorCode);
        status->set_errormessage(errorMessage);
    }

    class AerospikeDriver final : public Driver {

    public:

        explicit AerospikeDriver(const json &conf, const std::string &lua_dir) : Driver() {

            as_log_set_callback(glog_callback);


            // Initialize default lua configuration.
            as_config_lua lua;
            as_config_lua_init(&lua);

            if (!lua_dir.empty()) {

                if (lua_dir.length() < (AS_CONFIG_PATH_MAX_SIZE - 1)) {
                    strncpy(lua.user_path, lua_dir.c_str(), AS_CONFIG_PATH_MAX_SIZE);
                } else {
                    LOG(ERROR) << "lua_dir is too long: " << lua_dir;
                }

            }
            else {
                LOG(INFO) << "lua_dir is empty ";
            }

            // Initialize global lua configuration.
            aerospike_init_lua(&lua);

            as_config config;
            as_config_init(&config);

            _namespace = conf["namespace"];

            json cluster = conf["cluster"];

            std::string host = cluster["host"].get<std::string>();
            int port = cluster["port"].get<int>();

            if (! as_config_add_hosts(&config, host.c_str(), (uint16_t) port)) {
                LOG(ERROR) << "invalid host: " << host;
                throw std::invalid_argument( "invalid host" );
            }

            std::string username = cluster["username"].get<std::string>();
            std::string password = cluster["password"].get<std::string>();

            as_config_set_user(&config, username.c_str(), password.c_str());

            //memcpy(&config.tls, &g_tls, sizeof(as_config_tls));
            //config.auth_mode = g_auth_mode;

            aerospike_init(&_as, &config);

            as_error err;

            if (aerospike_connect(&_as, &err) != AEROSPIKE_OK) {
                LOG(ERROR) << "aerospike_connect code: " << err.code << ", message:" << err.message << host;
                throw std::invalid_argument( "aerospike_connect" );
            }

        }

        ~AerospikeDriver() override {

            as_error err;

            // Disconnect from the database cluster and clean up the aerospike object.
            aerospike_close(&_as, &err);
            aerospike_destroy(&_as);

            std::cout << "Graceful shutdown aerospike connection" << std::endl;

        }

        void getHead(const ::gkvs::KeyOperation *request, ::gkvs::HeadResult *response) override {


            as_key key;

            if (!init_key(request->key(), key, response->mutable_status())) {
                return;
            }

            as_record* rec = NULL;
            as_error err;

            as_status status = aerospike_key_exists(&_as, &err, NULL, &key, &rec);

            if (status == AEROSPIKE_OK) {

                if (rec) {
                    fill_head(rec, response->mutable_head());
                    as_record_destroy(rec);
                }

                success(response->mutable_status());

            }
            else if (status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {

                // return no head, means no record was found, this is not an error, it is like a map interface for records
                // not like database interface

                success(response->mutable_status());
            }
            else {
                error(err, response->mutable_status());
            }

        }

        void multiGetHead(const ::gkvs::BatchKeyOperation *request,
                          ::grpc::ServerWriter<::gkvs::HeadResult> *writer) override {

        }

        void get(const ::gkvs::KeyOperation *request, ::gkvs::RecordResult *response) override {

            as_key key;

            const Key& recordKey = request->key();

            if (!init_key(recordKey, key, response->mutable_status())) {
                return;
            }

            as_record* rec = NULL;
            as_error err;
            as_status status;

            int size = recordKey.columnkey_size();
            if (size == 0) {

                status = aerospike_key_get(&_as, &err, NULL, &key, &rec);

            }
            else {
                const char** bins = allocate_bins(recordKey);
                status = aerospike_key_select(&_as, &err, NULL, &key, bins, &rec);
                delete [] bins;
            }


            if (status == AEROSPIKE_OK) {

                if (rec) {
                    fill_record(rec, response->mutable_record());
                    as_record_destroy(rec);

                }

                success(response->mutable_status());

            }
            else if (status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {

                // return no record, means no record was found, this is not an error, it is like a map interface for records
                // not like database interface

                success(response->mutable_status());
            }
            else {
                error(err, response->mutable_status());
            }

        }

        void multiGet(const ::gkvs::BatchKeyOperation *request,
                      ::grpc::ServerWriter<::gkvs::RecordResult> *writer) override {

        }

        void scanHead(const ::gkvs::ScanOperation *request, ::grpc::ServerWriter<::gkvs::HeadResult> *writer) override {

        }

        void scan(const ::gkvs::ScanOperation *request, ::grpc::ServerWriter<::gkvs::RecordResult> *writer) override {

        }

        void put(const ::gkvs::PutOperation *request, ::gkvs::Status *response) override {

        }

        void compareAndPut(const ::gkvs::PutOperation *request, ::gkvs::Status *response) override {

        }

        void putAll(::grpc::ServerReaderWriter<::gkvs::Status, ::gkvs::PutOperation> *stream) override {

        }

        void remove(const ::gkvs::KeyOperation *request, ::gkvs::Status *response) override {

        }

        void removeAll(const ::gkvs::BatchKeyOperation *request, ::gkvs::Status *response) override {

        }


    private:

        aerospike _as;
        std::string _namespace;


    protected:

        void success(Status *status) {
            status->set_code(Status_Code_SUCCESS);
        }

        void bad_request(const char* errorMessage, Status *status) {
            status->set_code(Status_Code_ERROR_BAD_REQUEST);
            status->set_errorcode(Status_Code_ERROR_BAD_REQUEST);
            status->set_errormessage(errorMessage);
        }

        void error(as_error &err, gkvs::Status* status) {

            std::string errorMessage = err.message;

            status->set_code(parse_aerospike_status(err.code));
            status->set_errorcode(err.code);
            status->set_errormessage(errorMessage);

        }

        void driver_error(const char* errorMessage, Status *status) {
            status->set_code(Status_Code_ERROR_DRIVER);
            status->set_errorcode(Status_Code_ERROR_DRIVER);
            status->set_errormessage(errorMessage);
        }

        bool init_key(const Key &requestKey, as_key& key, Status* status) {

            const std::string& tableName = requestKey.tablename();

            switch(requestKey.recordRef_case()) {

                case Key::RecordRefCase::kRecordKey:
                    if (!as_key_init_str(&key, _namespace.c_str(), tableName.c_str(), requestKey.recordkey().c_str())) {
                        driver_error("as_key_init_str fail", status);
                        return false;
                    }
                    break;

                case Key::RecordRefCase::kRecordHash: {
                    //uint8_t digest[AS_DIGEST_VALUE_SIZE];
                    if (requestKey.recordhash().length() != AS_DIGEST_VALUE_SIZE) {
                        bad_request("record_hash must be 20 bytes", status);
                        return false;
                    }
                    const uint8_t *hash = (const uint8_t *) requestKey.recordhash().c_str();
                    if (!as_key_init_digest(&key, _namespace.c_str(), tableName.c_str(), hash)) {
                        driver_error("as_key_init_digest fail", status);
                        return false;
                    }
                    break;
                }

                default:
                    bad_request("no record_key", status);
                    return false;

            }

            return true;
        }


        const char** allocate_bins(const Key& key) {

            int size = key.columnkey_size();

            const char** bins = new const char*[size+1];

            for (int i = 0; i != size; ++i) {
                bins[i] = key.columnkey(i).c_str();
            }

            bins[size] = NULL;

            return bins;
        }

        void fill_head(as_record* rec, Head* head) {

            head->set_version(rec->gen);

            //uint16_t num_bins = as_record_numbins(rec);

            as_record_iterator it;
            as_record_iterator_init(&it, rec);

            while (as_record_iterator_has_next(&it)) {
                const as_bin* bin = as_record_iterator_next(&it);
                head->add_columnkey(as_bin_get_name(bin));
            }

        }

        void fill_record(as_record* rec, Record* record) {

            record->set_version(rec->gen);

            as_record_iterator it;
            as_record_iterator_init(&it, rec);

            auto columns = record->mutable_columns();

            while (as_record_iterator_has_next(&it)) {
                const as_bin* bin = as_record_iterator_next(&it);
                std::string columnKey(as_bin_get_name(bin));
                as_bin_value* value = as_bin_get_value(bin);

                if (value) {

                    // if bin has null value, we do not return pair key/value

                    as_val_t type = as_val_type(value);

                    if (type == AS_BYTES) {
                        as_bytes bytes = value->bytes;
                        std::string columnValue(reinterpret_cast<char const *>(bytes.value),
                                                bytes.size);
                        (*columns)[columnKey] = columnValue;
                    } else {
                        char *strValue = as_val_tostring(value);
                        if (strValue) {

                            // if value can not be converted to string, then we ignore it

                            std::string columnValue(strValue);
                            (*columns)[columnKey] = columnValue;
                        }
                    }
                }
            }

        }

    };

    Driver* create_aerospike_driver(const json &conf, const std::string &lua_path) {
        return new AerospikeDriver(conf, lua_path);
    }


}

static bool glog_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{

    va_list ap;
    va_start(ap, fmt);
    // allocate in heap to avoid stack overflow by untrusted vsnprintf function
    char *str = new char[AS_MAX_LOG_STR];
    int affected = vsnprintf(str, AS_MAX_LOG_STR, fmt, ap);
    if (affected > 0) {

        switch(level) {

            case AS_LOG_LEVEL_ERROR:
                LOG(ERROR) << str << std::endl;
                break;

            case AS_LOG_LEVEL_WARN:
                LOG(WARNING) << str << std::endl;
                break;

            case AS_LOG_LEVEL_INFO:
                LOG(INFO) << str << std::endl;
                break;

            case AS_LOG_LEVEL_DEBUG:
                DLOG(INFO) << str << std::endl;
                break;

            case AS_LOG_LEVEL_TRACE:
                VLOG(0) << str << std::endl;
                break;

            default:
                LOG(ERROR) << "unknown log level: " << level << ", msg: " << str << std::endl;
                break;

        }

    }
    va_end(ap);
    delete [] str;
    return true;
}


static gkvs::Status_Code parse_aerospike_status(as_status status) {

    switch(status) {

        case AEROSPIKE_OK:
            return gkvs::Status_Code::Status_Code_SUCCESS;

        case AEROSPIKE_NO_MORE_RECORDS:
        case AEROSPIKE_QUERY_END:
            return gkvs::Status_Code::Status_Code_SUCCESS_END_STREAM;

        case AEROSPIKE_ERR_RECORD_GENERATION:
            return gkvs::Status_Code::Status_Code_SUCCESS_NOT_UPDATED;

        case AEROSPIKE_ERR_NAMESPACE_NOT_FOUND:
            return gkvs::Status_Code::Status_Code_ERROR_RES_NOT_FOUND;

        case AEROSPIKE_ERR_RECORD_KEY_MISMATCH:
        case AEROSPIKE_ERR_GEO_INVALID_GEOJSON:
        case AEROSPIKE_INVALID_COMMAND:
        case AEROSPIKE_INVALID_FIELD:
        case AEROSPIKE_ERR_BIN_NAME:
        case AEROSPIKE_ERR_RECORD_TOO_BIG:
        case AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE:
        case AEROSPIKE_ERR_BIN_NOT_FOUND:
        case AEROSPIKE_ERR_REQUEST_INVALID:
        case AEROSPIKE_ERR_PARAM:
        case AEROSPIKE_ERR_INDEX_FOUND:
        case AEROSPIKE_ERR_INDEX_NOT_FOUND:
        case AEROSPIKE_ERR_INDEX_NAME_MAXLEN:
        case AEROSPIKE_ERR_INDEX_MAXCOUNT:
        case AEROSPIKE_ERR_UDF_NOT_FOUND:
        case AEROSPIKE_ERR_LUA_FILE_NOT_FOUND:
            return gkvs::Status_Code::Status_Code_ERROR_BAD_REQUEST;

        case AEROSPIKE_ERR_BIN_EXISTS:
        case AEROSPIKE_ERR_RECORD_EXISTS:
        case AEROSPIKE_ERR_RECORD_NOT_FOUND:
        case AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS:
        case AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND:
            return gkvs::Status_Code::Status_Code_ERROR_POLICY;

        case AEROSPIKE_ERR_CLUSTER_CHANGE:
            return gkvs::Status_Code::Status_Code_ERROR_MIGRATION;

        case AEROSPIKE_ERR_CLUSTER:
        case AEROSPIKE_ERR_INVALID_HOST:
        case AEROSPIKE_ERR_INVALID_NODE:
        case AEROSPIKE_ERR_NO_MORE_CONNECTIONS:
        case AEROSPIKE_ERR_ASYNC_CONNECTION:
        case AEROSPIKE_ERR_CONNECTION:
            return gkvs::Status_Code::Status_Code_ERROR_NETWORK;

        case AEROSPIKE_SECURITY_NOT_SUPPORTED:
        case AEROSPIKE_SECURITY_NOT_ENABLED:
        case AEROSPIKE_SECURITY_SCHEME_NOT_SUPPORTED:
        case AEROSPIKE_ILLEGAL_STATE:
        case AEROSPIKE_INVALID_USER:
        case AEROSPIKE_USER_ALREADY_EXISTS:
        case AEROSPIKE_INVALID_PASSWORD:
        case AEROSPIKE_EXPIRED_PASSWORD:
        case AEROSPIKE_FORBIDDEN_PASSWORD:
        case AEROSPIKE_INVALID_CREDENTIAL:
        case AEROSPIKE_INVALID_ROLE:
        case AEROSPIKE_ROLE_ALREADY_EXISTS:
        case AEROSPIKE_INVALID_PRIVILEGE:
            return gkvs::Status_Code::Status_Code_ERROR_AUTH;

        case AEROSPIKE_ERR_FAIL_FORBIDDEN:
        case AEROSPIKE_ERR_ALWAYS_FORBIDDEN:
        case AEROSPIKE_ERR_TLS_ERROR:
        case AEROSPIKE_NOT_AUTHENTICATED:
        case AEROSPIKE_ROLE_VIOLATION:
            return gkvs::Status_Code::Status_Code_ERROR_FORBIDDEN;

        case AEROSPIKE_ERR_QUERY_TIMEOUT:
        case AEROSPIKE_ERR_TIMEOUT:
            return gkvs::Status_Code::Status_Code_ERROR_TIMEOUT;

        case AEROSPIKE_ERR_BATCH_QUEUES_FULL:
        case AEROSPIKE_ERR_DEVICE_OVERLOAD:
        case AEROSPIKE_ERR_ASYNC_QUEUE_FULL:
        case AEROSPIKE_ERR_QUERY_QUEUE_FULL:
        case AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED:
            return gkvs::Status_Code::Status_Code_ERROR_OVERLOAD;

        case AEROSPIKE_ERR_SERVER_FULL:
        case AEROSPIKE_ERR_INDEX_OOM:
            return gkvs::Status_Code::Status_Code_ERROR_OVERFLOW;

        case AEROSPIKE_ERR_RECORD_BUSY:
            return gkvs::Status_Code::Status_Code_ERROR_LOCKED;

        case AEROSPIKE_ERR_SCAN_ABORTED:
        case AEROSPIKE_ERR_CLIENT_ABORT:
        case AEROSPIKE_ERR_QUERY_ABORTED:
            return gkvs::Status_Code::Status_Code_ERROR_ABORTED;

        case AEROSPIKE_ERR_UNSUPPORTED_FEATURE:
        case AEROSPIKE_ERR_BATCH_DISABLED:
            return gkvs::Status_Code::Status_Code_ERROR_UNSUPPORTED;

        case AEROSPIKE_ERR_CLIENT:
        case AEROSPIKE_ERR_SERVER:
        case AEROSPIKE_ERR_INDEX_NOT_READABLE:
        case AEROSPIKE_ERR_INDEX:
        case AEROSPIKE_ERR_QUERY:
        case AEROSPIKE_ERR_UDF:
            return gkvs::Status_Code::Status_Code_ERROR_DRIVER;

        default:
            return gkvs::Status_Code::Status_Code_ERROR_INTERNAL;


    }

}


