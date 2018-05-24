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
#include <aerospike/as_batch.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_scan.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_query.h>
#include <aerospike/aerospike_query.h>

#include <glog/logging.h>

#include "driver.h"
#include "as_driver.h"
#include "crypto.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;


namespace gkvs {


    class StatusErr final {

    public:

        void bad_request(const char* errorMessage) {
            _statusCode = Status_Code_ERROR_INTERNAL;
            _errorCode = AEROSPIKE_ERR;
            _errorMessage = errorMessage;
        }

        void driver_error(const char* errorMessage) {
            _statusCode = Status_Code_ERROR_DRIVER;
            _errorCode = AEROSPIKE_ERR;
            _errorMessage = errorMessage;
        }

        void to_status(Status* status) {
            status->set_code(_statusCode);
            status->set_errorcode(_errorCode);
            status->set_errormessage(_errorMessage);
        }

    private:

        Status_Code _statusCode = Status_Code_ERROR_INTERNAL;
        int _errorCode = AEROSPIKE_ERR;
        const char* _errorMessage = "";

    };


    class AerospikeDriver final : public Driver {

    public:

        explicit AerospikeDriver(const std::string &conf_str, const std::string &lua_dir) : Driver() {

            json conf = nlohmann::json::parse(conf_str.begin(), conf_str.end());

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


        void get(const KeyOperation *request, ValueResult *response) override {

            do_get(request, response);

        }

        void multiGet(const BatchKeyOperation *request, BatchValueResult *response) override {

            do_multi_get(request, response);

        }


        void getAll(::grpc::ServerReaderWriter<::gkvs::ValueResult, ::gkvs::KeyOperation> *stream) override {

            KeyOperation request;
            ValueResult response;

            if (stream->Read(&request)) {

                response.Clear();

                do_get(&request, &response);

                stream->Write(response);

            }

        }

        void scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer) override {

            do_scan(request, writer);

        }

        void put(const PutOperation *request, StatusResult *response) override {

            do_put(request, response);

        }

        void putAll(::grpc::ServerReaderWriter<StatusResult, PutOperation> *stream) override {

            PutOperation request;
            StatusResult response;

            if (stream->Read(&request)) {

                response.Clear();

                do_put(&request, &response);

                stream->Write(response);

            }

        }

        void remove(const KeyOperation *request, StatusResult *response) override {

            do_remove(request, response);

        }

        void removeAll(::grpc::ServerReaderWriter<StatusResult, KeyOperation> *stream) override {

            KeyOperation request;
            StatusResult response;

            if (stream->Read(&request)) {

                response.Clear();

                do_remove(&request, &response);

                stream->Write(response);

            }

        }


    private:

        aerospike _as;
        std::string _namespace;
        uint32_t _max_retries = 1;
        uint32_t _sleep_between_retries = 1;
        as_policy_consistency_level _consistency_level = AS_POLICY_CONSISTENCY_LEVEL_ALL;
        as_policy_commit_level _commit_level = AS_POLICY_COMMIT_LEVEL_ALL;
        as_policy_key _send_key = AS_POLICY_KEY_SEND;
        as_policy_replica _replica = AS_POLICY_REPLICA_SEQUENCE;
        uint32_t _min_concurrent_batch_size = 5;


    protected:

        void init_read_policy(const Operation& op, as_policy_read* pol) {

            as_policy_read_init(pol);

            if (op.timeout() > 0) {
                pol->base.total_timeout = static_cast<uint32_t>(op.timeout());
            }
            pol->base.max_retries = _max_retries;
            pol->base.sleep_between_retries = _sleep_between_retries;
            pol->key = _send_key;
            pol->replica = _replica;

            pol->consistency_level = _consistency_level;
        }


        void init_write_policy(const Operation& op, as_policy_write* pol) {

            as_policy_write_init(pol);

            if (op.timeout() > 0) {
                pol->base.total_timeout = static_cast<uint32_t>(op.timeout());
            }
            pol->base.max_retries = _max_retries;
            pol->base.sleep_between_retries = _sleep_between_retries;
            pol->key = _send_key;
            pol->replica = _replica;

            pol->exists = AS_POLICY_EXISTS_IGNORE;

            pol->commit_level = _commit_level;
        }

        void init_remove_policy(const Operation& op, as_policy_remove* pol) {

            as_policy_remove_init(pol);

            if (op.timeout() > 0) {
                pol->base.total_timeout = static_cast<uint32_t>(op.timeout());
            }
            pol->base.max_retries = _max_retries;
            pol->base.sleep_between_retries = _sleep_between_retries;
            pol->durable_delete = true;
            pol->replica = _replica;

            pol->commit_level = _commit_level;

        }

        void init_batch_policy(int totalTimeoutMillis, uint32_t actual_size, as_policy_batch* pol) {

            as_policy_batch_init(pol);

            if (totalTimeoutMillis > 0) {
                pol->base.total_timeout = static_cast<uint32_t>(totalTimeoutMillis);
            }

            pol->base.max_retries = _max_retries;
            pol->base.sleep_between_retries = _sleep_between_retries;
            pol->consistency_level = _consistency_level;

            pol->send_set_name = true;

            if (actual_size >= _min_concurrent_batch_size) {
                pol->concurrent = true;
            }

        }

        void init_scan_policy(int totalTimeoutMillis, as_policy_scan* pol) {

            as_policy_scan_init(pol);

            if (totalTimeoutMillis > 0) {
                pol->base.total_timeout = static_cast<uint32_t>(totalTimeoutMillis);
            }

            pol->base.max_retries = _max_retries;
            pol->base.sleep_between_retries = _sleep_between_retries;
            pol->fail_on_cluster_change = false;

        }

        void init_query_policy(int totalTimeoutMillis, as_policy_query* pol) {

            as_policy_query_init(pol);

            if (totalTimeoutMillis > 0) {
                pol->base.total_timeout = static_cast<uint32_t>(totalTimeoutMillis);
            }

            pol->base.max_retries = _max_retries;
            pol->base.sleep_between_retries = _sleep_between_retries;
            pol->deserialize = false;

        }

        void success(Status *status) {
            status->set_code(Status_Code_SUCCESS);
        }

        void bad_request(const char* errorMessage, Status *status) {
            status->set_code(Status_Code_ERROR_BAD_REQUEST);
            status->set_errorcode(Status_Code_ERROR_BAD_REQUEST);
            status->set_errormessage(errorMessage);
        }

        void unsupported(const char* errorMessage, Status *status) {
            status->set_code(Status_Code_ERROR_UNSUPPORTED);
            status->set_errorcode(Status_Code_ERROR_UNSUPPORTED);
            status->set_errormessage(errorMessage);
        }

        void driver_error(const char* errorMessage, Status *status) {
            status->set_code(Status_Code_ERROR_DRIVER);
            status->set_errorcode(AEROSPIKE_ERR);
            status->set_errormessage(errorMessage);
        }

        void error(as_error &err, Status* status) {
            std::string errorMessage = err.message;
            status->set_code(parse_aerospike_status(err.code));
            status->set_errorcode(err.code);
            status->set_errormessage(errorMessage);
        }

        void error(as_status code, Status* status) {
            status->set_code(parse_aerospike_status(code));
            status->set_errorcode(code);
        }

        bool valid_key(const Key &key, StatusErr& statusErr) {

            if (key.tablename().empty()) {
                statusErr.bad_request("empty table name");
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

        bool init_key(const Key &requestKey, as_key& key, StatusErr& statusErr) {

            const std::string& tableName = requestKey.tablename();

            if (tableName.empty()) {
                statusErr.bad_request("empty table name");
                return false;
            }

            switch(requestKey.recordKey_case()) {

                case Key::RecordKeyCase::kRaw: {
                    const uint8_t* value = (const uint8_t*) requestKey.raw().c_str();
                    uint32_t len = static_cast<uint32_t>(requestKey.raw().length());
                    if (!as_key_init_rawp(&key, _namespace.c_str(), tableName.c_str(), value, len, false)) {
                        statusErr.driver_error("as_key_init_raw fail");
                        return false;
                    }
                    break;
                }

                case Key::RecordKeyCase::kDigest: {
                    if (requestKey.digest().length() < AS_DIGEST_VALUE_SIZE) {
                        statusErr.bad_request("record digest must not be less than 20 bytes");
                        return false;
                    }
                    const uint8_t *value = (const uint8_t *) requestKey.digest().c_str();
                    if (!as_key_init_digest(&key, _namespace.c_str(), tableName.c_str(), value)) {
                        statusErr.driver_error("as_key_init_digest fail");
                        return false;
                    }
                    break;
                }

                default:
                    statusErr.bad_request("no recordRef");
                    return false;

            }

            return true;
        }


        const char** allocate_bins(const Select& select) {

            int size = select.column_size();

            const char ** bins = new const char*[size+1];

            size_t i = 0;
            for (auto &col : select.column()) {
                bins[i++] = col.c_str();
            }

            bins[size] = nullptr;

            return bins;
        }

        const char** allocate_bins(std::set<std::string>& select) {

            size_t size = select.size();

            const char ** bins = new const char*[size+1];

            size_t i = 0;
            for (auto &col : select) {
                bins[i++] = col.c_str();
            }

            bins[size] = nullptr;

            return bins;

        }

        void metadata_result(as_record *rec, ValueResult *result) {

            Metadata* meta = result->mutable_metadata();

            meta->set_version(rec->gen);
            meta->set_ttl(rec->ttl);

        }

        bool include_value(const OutputOptions &out) {

            switch(out) {
                case VALUE:
                case VALUE_DIGEST:
                case KEY_VALUE:
                case KEY_VALUE_DIGEST:
                    return true;
                default:
                    break;
            }

            return false;
        }

        bool include_value_digest(const OutputOptions &out) {

            switch(out) {
                case VALUE_DIGEST:
                case KEY_VALUE_DIGEST:
                    return true;
                default:
                    break;
            }

            return false;
        }

        bool include_key(const OutputOptions &out) {

            switch(out) {
                case KEY:
                case KEY_VALUE:
                case KEY_VALUE_DIGEST:
                    return true;
                default:
                    break;
            }

            return false;

        }


        void key_result(const Key& key , ValueResult *result, const OutputOptions &out) {

            if (include_key(out)) {
                result->mutable_key()->CopyFrom(key);
            }

        }

        /**
         * TODO: implement DIGEST calc
         */

        void value_result(as_record *rec, ValueResult *result, const OutputOptions &out) {

            bool includeValue = include_value(out);
            bool includeValueDigest = include_value_digest(out);

            as_record_iterator it;
            as_record_iterator_init(&it, rec);

            while (as_record_iterator_has_next(&it)) {

                const as_bin* bin = as_record_iterator_next(&it);

                Value* recordValue = result->add_value();

                std::string column(as_bin_get_name(bin));
                recordValue->set_column(column);

                as_bin_value* value = as_bin_get_value(bin);

                if (value && includeValue) {

                    as_val_t type = as_val_type(value);

                    if (type == AS_BYTES) {
                        as_bytes bytes = value->bytes;

                        if (includeValueDigest) {
                            Ripend160Hash hash;
                            hash.apply(bytes.value, bytes.size);
                            recordValue->set_raw(hash.data(), hash.size());
                        }
                        else {
                            recordValue->set_raw(bytes.value, bytes.size);
                        }

                    } else {
                        char *pstr = as_val_tostring(value);
                        // if value can not be converted to string, then we ignore it
                        if (pstr) {

                            if (includeValueDigest) {
                                Ripend160Hash hash;
                                hash.apply(pstr);
                                recordValue->set_raw(hash.data(), hash.size());
                            }
                            else {
                                recordValue->set_raw(pstr);
                            }
                            cf_free(pstr);
                        }
                    }
                }


            }

        }

        void key_result(as_key* key, ValueResult *result, const OutputOptions &out) {

            if (!include_key(out)) {
                return;
            }

            Key* res = result->mutable_key();
            res->set_tablename(key->set);

            bool setup = false;

            if (key->valuep) {

                as_key_value* value = key->valuep;
                as_val_t type = as_val_type(value);

                if (type == AS_BYTES) {
                    as_bytes bytes = value->bytes;
                    res->set_raw(bytes.value, bytes.size);
                    setup = true;
                } else {
                    char *pstr = as_val_tostring(value);
                    // if value can not be converted to string, then we ignore it
                    if (pstr) {
                        res->set_raw(pstr);
                        setup = true;
                        cf_free(pstr);
                    }
                }

            }

            if (!setup) {

                as_digest *digest = as_key_digest(key);
                if (digest) {
                    res->set_digest(key->digest.value, AS_DIGEST_VALUE_SIZE);
                }

            }

        }


        /**
         * MULTI_GET
         */

        struct multiGet_context {

            AerospikeDriver* instance;
            BatchValueResult *response;
            std::unordered_map<const as_key*, const KeyOperation*, as_key_hash, as_key_equal> key_map;

        };

        void do_multi_get(const BatchKeyOperation *request, BatchValueResult *response);

        bool multiGet_callback(const as_batch_read* results, uint32_t n, multiGet_context* context);

        static bool static_multiGet_callback(const as_batch_read* results, uint32_t n, void* udata);


        /**
         * SCAN
         */

        struct scan_context {

            AerospikeDriver* instance;
            grpc::ServerWriter<::gkvs::ValueResult> *writer;
            const ScanOperation* operation;
            mutable std::mutex scan_mutex;

        };

        void do_scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer);

        bool static static_scan_callback(const as_val* val, void* udata);

        bool scan_callback(const as_val* val, scan_context* context);

        /**
         * SIMPLE
         */


        void do_get(const KeyOperation *request, ValueResult *response);

        void do_put(const PutOperation *request, StatusResult *response);

        void do_remove(const KeyOperation *request, StatusResult *response);


    };

    Driver* create_aerospike_driver(const std::string &conf_str, const std::string &lua_path) {
        return new AerospikeDriver(conf_str, lua_path);
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



void gkvs::AerospikeDriver::do_multi_get(const ::gkvs::BatchKeyOperation *request, ::gkvs::BatchValueResult *response) {

    multiGet_context context = {this, response};

    uint32_t size = static_cast<uint32_t>(request->operation().size());

    as_batch batch;
    as_batch_inita(&batch, size);

    bool includeValue = false;
    bool useSelect = true;
    std::set<std::string> select;

    int max_timeout = 0;
    uint32_t actual_size = 0;

    for (uint32_t i = 0; i < size; ++i) {

        const KeyOperation &operation = request->operation(i);

        if (!operation.has_key()) {
            ValueResult *result = response->add_result();
            result->set_sequencenum(operation.sequencenum());
            bad_request("empty key", result->mutable_status());
            continue;
        }

        if (!operation.has_op()) {
            ValueResult *result = response->add_result();
            result->set_sequencenum(operation.sequencenum());
            bad_request("empty op", result->mutable_status());
            continue;
        }

        StatusErr statusErr;
        if (!valid_key(operation.key(), statusErr)) {
            ValueResult *result = response->add_result();
            result->set_sequencenum(operation.sequencenum());
            statusErr.to_status(result->mutable_status());
            continue;
        }

        if (operation.op().timeout() > max_timeout) {
            max_timeout = operation.op().timeout();
        }

        as_key *key = as_batch_keyat(&batch, i);
        if (!init_key(operation.key(), *key, statusErr)) {
            ValueResult *result = response->add_result();
            result->set_sequencenum(operation.sequencenum());
            statusErr.to_status(result->mutable_status());
            continue;
        }

        if (operation.has_select()) {
            if (useSelect) {
                for (auto &col : operation.select().column()) {
                    select.insert(col);
                }
            }
        }
        else {
            // if no select specified, return the whole record
            useSelect = false;
        }

        includeValue |= include_value(operation.output());
        context.key_map[key] = &operation;
        actual_size++;
    }

    if (actual_size < size) {
        batch.keys.size = actual_size;
    }

    as_policy_batch pol;
    init_batch_policy(max_timeout, actual_size, &pol);

    as_error err;
    if (includeValue) {

        if (useSelect) {
            const char** bins = allocate_bins(select);
            uint32_t n_bins = static_cast<uint32_t>(select.size());
            aerospike_batch_get_bins(&_as, &err, &pol, &batch, bins, n_bins, static_multiGet_callback, &context);
            delete[] bins;
        }
        else {
            aerospike_batch_get(&_as, &err, &pol, &batch, static_multiGet_callback, &context);
        }
    }
    else {
        aerospike_batch_exists(&_as, &err, &pol, &batch, static_multiGet_callback, &context);
    }

    as_batch_destroy(&batch);

}



bool gkvs::AerospikeDriver::static_multiGet_callback(const as_batch_read* results, uint32_t n, void* udata) {

    multiGet_context* context = (multiGet_context*) udata;
    return context->instance->multiGet_callback(results, n, context);
}


bool gkvs::AerospikeDriver::multiGet_callback(const as_batch_read* results, uint32_t n, multiGet_context* context) {

    BatchValueResult *response = context->response;

    for (uint32_t i = 0; i < n; ++i) {

        ValueResult *result = response->add_result();

        const KeyOperation* operation = nullptr;
        const as_key* key = results[i].key;
        if (key) {
            operation = context->key_map[key];
        }

        if (operation) {
            // for client identification purpose
            result->set_sequencenum(operation->sequencenum());
            key_result(operation->key(), result, operation->output());
        }
        else {
            char* pstr = as_val_tostring(key->valuep);
            LOG(ERROR) << "operation not found for key: " << pstr << std::endl;
            cf_free(pstr);
        }

        as_status status = results[i].result;

        if (status == AEROSPIKE_OK) {

            const as_record *rec = &results[i].record;

            metadata_result(const_cast<as_record *>(rec), result);
            if (operation) {
                value_result(const_cast<as_record *>(rec), result, operation->output());
            }
            else {
                value_result(const_cast<as_record *>(rec), result, OutputOptions::VALUE);
            }

            success(result->mutable_status());

        }
        else if (status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {

            // return no metadata, that means record was not found

            success(result->mutable_status());
        }
        else {
            error(results[i].result, result->mutable_status());
        }

    }

    return true;
}


void gkvs::AerospikeDriver::do_scan(const ::gkvs::ScanOperation *request, ::grpc::ServerWriter<::gkvs::ValueResult> *writer) {

    const std::string& tableName = request->tablename();

    if (tableName.empty()) {

        ValueResult result;
        bad_request("empty table name", result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
        return;
    }

    as_error err;
    scan_context context { this, writer, request };

    if (request->has_bucket()) {

        as_query query;
        as_query_init(&query, _namespace.c_str(), tableName.c_str());

        if (!include_value(request->output())) {
            query.no_bins = true;
        }

        if (request->has_select()) {
            uint16_t len = static_cast<uint16_t>(request->select().column().size());
            as_query_select_init(&query, len);

            for (auto &col : request->select().column()) {
                as_query_select(&query, col.c_str());
            }

        }

        const Bucket& bucket = request->bucket();
        as_query_predexp_init(&query, 3);
        as_query_predexp_add(&query, as_predexp_rec_digest_modulo(bucket.totalnum()));
        as_query_predexp_add(&query, as_predexp_integer_value(bucket.bucketnum()));
        as_query_predexp_add(&query, as_predexp_integer_equal());

        as_policy_query pol;
        init_query_policy(request->op().timeout(), &pol);

        aerospike_query_foreach(&_as, &err, &pol, &query, static_scan_callback, &context);

        as_query_destroy(&query);

    }
    else {

        as_scan scan;
        as_scan_init(&scan, _namespace.c_str(), tableName.c_str());

        if (!include_value(request->output())) {
            as_scan_set_nobins(&scan, true);
        }

        scan.priority = AS_SCAN_PRIORITY_LOW;

        if (request->has_select()) {
            uint16_t len = static_cast<uint16_t>(request->select().column().size());
            as_scan_select_init(&scan, len);

            for (auto &col : request->select().column()) {
                as_scan_select(&scan, col.c_str());
            }

        }

        as_policy_scan pol;
        init_scan_policy(request->op().timeout(), &pol);

        aerospike_scan_foreach(&_as, &err, &pol, &scan, static_scan_callback, &context);

        as_scan_destroy(&scan);

    }


}

bool gkvs::AerospikeDriver::static_scan_callback(const as_val* val, void* udata) {

    scan_context* context = (scan_context*) udata;
    return context->instance->scan_callback(val, context);

}

bool gkvs::AerospikeDriver::scan_callback(const as_val* val, scan_context* context) {

    grpc::ServerWriter<::gkvs::ValueResult> *writer = context->writer;
    const ScanOperation* operation = context->operation;

    ValueResult result;

    as_record* rec = as_record_fromval(val);
    if (rec) {

        // no sequence num
        metadata_result(rec, &result);
        key_result(&rec->key, &result, operation->output());
        value_result(rec, &result, operation->output());


        std::lock_guard< std::mutex > guard( context->scan_mutex );
        writer->Write(result, grpc::WriteOptions());
    }

    return true;
}


void gkvs::AerospikeDriver::do_get(const ::gkvs::KeyOperation *request, ::gkvs::ValueResult *response) {

    // for client identification purpose
    response->set_sequencenum(request->sequencenum());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    // for client identification purpose
    key_result(request->key(), response, request->output());

    if (!request->has_op()) {
        bad_request("no op", response->mutable_status());
        return;
    }

    StatusErr statusErr;
    if (!valid_key(request->key(), statusErr)) {
        statusErr.to_status(response->mutable_status());
        return;
    }


    as_key key;
    if (!init_key(request->key(), key, statusErr)) {
        statusErr.to_status(response->mutable_status());
        return;
    }

    as_policy_read pol;
    init_read_policy(request->op(), &pol);

    as_record* rec = nullptr;
    as_error err;
    as_status status;

    if (include_value(request->output())) {
        if (request->has_select()) {
            const char** bins = allocate_bins(request->select());
            status = aerospike_key_select(&_as, &err, &pol, &key, bins, &rec);
            delete [] bins;
        }
        else {
            status = aerospike_key_get(&_as, &err, &pol, &key, &rec);
        }
    }
    else {
        status = aerospike_key_exists(&_as, &err, &pol, &key, &rec);
    }

    if (status == AEROSPIKE_OK) {

        if (rec) {

            metadata_result(rec, response);
            value_result(rec, response, request->output());

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

void gkvs::AerospikeDriver::do_put(const ::gkvs::PutOperation *request, ::gkvs::StatusResult *response) {

    response->set_sequencenum(request->sequencenum());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    if (!request->has_op()) {
        bad_request("no op", response->mutable_status());
        return;
    }

    StatusErr statusErr;
    if (!valid_key(request->key(), statusErr)) {
        statusErr.to_status(response->mutable_status());
        return;
    }

    as_key key;

    if (!init_key(request->key(), key, statusErr)) {
        statusErr.to_status(response->mutable_status());
        return;
    }

    auto size = static_cast<uint16_t>(request->value().size());

    as_record* rec = as_record_new(size);

    for (const Value &val : request->value()) {

        if (val.value_case() != Value::ValueCase::kRaw) {
            bad_request("value must be raw", response->mutable_status());
            return;
        }

        const uint8_t* raw = (const uint8_t*) val.raw().c_str();
        uint32_t len = static_cast<uint32_t>(val.raw().size());

        // save all bins as raw bytes
        as_record_set_raw(rec, val.column().c_str(), raw, len);
    }

    as_policy_write pol;
    init_write_policy(request->op(), &pol);

    int ttl = request->ttl();
    if (ttl > 0) {
        rec->ttl = static_cast<uint32_t>(ttl);
    }

    // save the key if sent
    if (request->key().recordKey_case() == Key::RecordKeyCase::kRaw) {
        pol.key = AS_POLICY_KEY_SEND;
    }

    // set version for CompareAndPut
    if (request->compareandput()) {
        pol.gen = AS_POLICY_GEN_EQ;
        rec->gen = static_cast<uint16_t>(request->version());
    }
    else {
        pol.gen = AS_POLICY_GEN_IGNORE;
    }

    as_error err;
    as_status status = aerospike_key_put(&_as, &err, &pol, &key, rec);

    if (status == AEROSPIKE_OK) {

        success(response->mutable_status());

    }
    else if (status == AEROSPIKE_ERR_RECORD_GENERATION && request->compareandput()) {

        response->mutable_status()->set_code(Status_Code_SUCCESS_NOT_UPDATED);

    }
    else {
        error(err, response->mutable_status());
    }

    as_record_destroy(rec);
}

void gkvs::AerospikeDriver::do_remove(const ::gkvs::KeyOperation *request, ::gkvs::StatusResult *response) {

    response->set_sequencenum(request->sequencenum());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    if (!request->has_op()) {
        bad_request("no op", response->mutable_status());
        return;
    }

    if (request->has_select()) {
        unsupported("selected columns can not be deleted", response->mutable_status());
        return;
    }

    StatusErr statusErr;
    if (!valid_key(request->key(), statusErr)) {
        statusErr.to_status(response->mutable_status());
        return;
    }

    as_key key;
    if (!init_key(request->key(), key, statusErr)) {
        statusErr.to_status(response->mutable_status());
        return;
    }

    as_policy_remove pol;
    init_remove_policy(request->op(), &pol);

    as_error err;
    as_status status;

    status = aerospike_key_remove(&_as, &err, &pol, &key);

    if (status == AEROSPIKE_OK) {

        success(response->mutable_status());

    }
    else {
        error(err, response->mutable_status());
    }

}