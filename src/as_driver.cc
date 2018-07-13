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
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>

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
#include <aerospike/as_nil.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_stringmap.h>

#include <glog/logging.h>

#include "driver.h"
#include "as_driver.h"
#include "crypto.h"


#define AS_MAX_LOG_STR 1024
static bool glog_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...);

static gkvs::StatusCode parse_aerospike_status(as_status status);

namespace gkvs {

    static bool to_record_callback (const as_val * key, const as_val * value, void * udata);

    class AerospikeSet final {

    public:

        explicit AerospikeSet(const std::string& set) : set_(set) {
        }

        bool configure(const json& conf, std::string& error) {

            auto i = conf.find("namespace");

            if (i == conf.end()) {
                error = "namespace is empty";
                return false;
            }

            namespace_ = i->get<std::string>();

            i = conf.find("single_bin");

            if (i != conf.end()) {
                single_bin_ = i->get<bool>();
            }

            i = conf.find("ttl");

            if (i != conf.end()) {
                ttl_ = i->get<int>();
            }

            i = conf.find("timeout");

            if (i != conf.end()) {
                timeout_ = i->get<int>();
            }

            return true;

        }

        const std::string& get_set() const {
            return set_;
        }

        const std::string& get_namespace() const {
            return namespace_;
        }

        bool is_single_bin() const {
            return single_bin_;
        }

        int get_ttl() const {
            return ttl_;
        }

        int get_timeout() const {
            return timeout_;
        }

    private:

        std::string set_;
        std::string namespace_;
        bool single_bin_;
        int ttl_;
        int timeout_;

    };

    class AerospikeDriver final : public Driver {

    public:

        explicit AerospikeDriver(const std::string& name, const json &conf, const std::string &lua_dir) : Driver(name) {

            LOG(INFO) << "aerospike conf = " << conf << std::endl;

            //json conf = nlohmann::json::parse(conf_str.begin(), conf_str.end());

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
            config.fail_if_not_connected = false;

            auto hosts_it = conf.find("hosts");

            if (hosts_it != conf.end()) {

                json hosts = *hosts_it;

                for (json::iterator server_it = hosts.begin(); server_it != hosts.end(); ++server_it) {

                    json server = *server_it;

                    auto host_it =  server.find("host");
                    auto port_it = server.find("port");

                    std::string host = host_it != server.end() ? host_it->get<std::string>() : "127.0.0.1";
                    int port = port_it != server.end() ? port_it->get<int>() : 3000;

                    LOG(INFO) << "Aerospike connect to " << host << ":" << port << std::endl;

                    if (! as_config_add_hosts(&config, host.c_str(), (uint16_t) port)) {
                        LOG(ERROR) << "invalid endpoint: " << host << ":" << port << std::endl;
                    }

                }


            }

            auto user_it = conf.find("user");
            auto pass_it = conf.find("password");

            std::string username = user_it != conf.end() ? user_it->get<std::string>() : "";
            std::string password = pass_it != conf.end() ? pass_it->get<std::string>() : "";

            as_config_set_user(&config, username.c_str(), password.c_str());

            auto tls_it = conf.find("tls");
            if (tls_it != conf.end()) {
                json tls = *tls_it;

                std::cout << "tls: " << tls << std::endl;

                config.tls.enable = true;
                config.tls.log_session_info = true;

                as_config_tls_set_cafile(&config, tls["cafile"].get<std::string>().c_str());
                as_config_tls_set_capath(&config, tls["capath"].get<std::string>().c_str());

                config.tls.crl_check = true;
                config.tls.crl_check_all = true;

                as_config_tls_set_keyfile(&config, tls["keyfile"].get<std::string>().c_str());
                as_config_tls_set_certfile(&config, tls["certfile"].get<std::string>().c_str());

                config.tls.for_login_only = tls["login_only"].get<bool>();
                config.auth_mode = AS_AUTH_EXTERNAL;
            }
            else {
                config.auth_mode = AS_AUTH_INTERNAL;
            }

            aerospike_init(&as_, &config);

            as_error err;

            if (aerospike_connect(&as_, &err) != AEROSPIKE_OK) {
                LOG(ERROR) << "aerospike_connect code: " << err.code << ", message:" << err.message << std::endl;
                throw std::invalid_argument( "aerospike_connect" );
            }

        }

        ~AerospikeDriver() override {

            as_error err;

            // Disconnect from the database cluster and clean up the aerospike object.
            aerospike_close(&as_, &err);
            aerospike_destroy(&as_);

            std::cout << "Graceful shutdown aerospike connection" << std::endl;

        }

        bool add_table(const std::string& table, const json& conf, std::string& error) override {

            auto i = sets_.find(table);

            if (i != sets_.end()) {
                error = "table '" + table +"' already exists in " + get_name();
                return false;
            }

            std::shared_ptr<AerospikeSet> set(new AerospikeSet(table));

            if (!set->configure(conf, error)) {
                return false;
            }

            sets_[table] = set;

            return true;
        }

        void get(const KeyOperation *request, const std::string& table_override, ValueResult *response) override {

            do_get(request, table_override, response);

        }

        void multiGet(const std::vector<MultiGetEntry>& entries) override {

            do_multi_get(entries);

        }

        void scan(const ScanOperation *request, const std::string& table_override, ::grpc::ServerWriter<ValueResult> *writer) override {

            do_scan(request, table_override, writer);

        }

        void put(const PutOperation *request, const std::string& table_override, StatusResult *response) override {

            do_put(request, table_override, response);

        }

        void remove(const KeyOperation *request, const std::string& table_override, StatusResult *response) override {

            do_remove(request, table_override, response);

        }


    private:

        std::unordered_map<std::string, std::shared_ptr<AerospikeSet>> sets_;

        aerospike as_;
        uint32_t max_retries_ = 1;
        uint32_t sleep_between_retries_ = 1;
        as_policy_consistency_level consistency_level_ = AS_POLICY_CONSISTENCY_LEVEL_ALL;
        as_policy_commit_level commit_level_ = AS_POLICY_COMMIT_LEVEL_ALL;
        as_policy_key send_key_ = AS_POLICY_KEY_SEND;
        as_policy_replica replica_ = AS_POLICY_REPLICA_SEQUENCE;
        uint32_t min_concurrent_batch_size_ = 5;
        bool durable_delete_ = false;


    protected:

        void init_read_policy(uint32_t timeout, as_policy_read* pol) {

            as_policy_read_init(pol);

            if (timeout > 0) {
                pol->base.total_timeout = timeout;
            }
            pol->base.max_retries = max_retries_;
            pol->base.sleep_between_retries = sleep_between_retries_;
            pol->key = send_key_;
            pol->replica = replica_;

            pol->consistency_level = consistency_level_;
        }


        void init_write_policy(uint32_t timeout, as_policy_write* pol) {

            as_policy_write_init(pol);

            if (timeout > 0) {
                pol->base.total_timeout = timeout;
            }
            pol->base.max_retries = max_retries_;
            pol->base.sleep_between_retries = sleep_between_retries_;
            pol->key = send_key_;
            pol->replica = replica_;

            pol->exists = AS_POLICY_EXISTS_IGNORE;

            pol->commit_level = commit_level_;
        }

        void init_remove_policy(uint32_t timeout, as_policy_remove* pol) {

            as_policy_remove_init(pol);

            if (timeout > 0) {
                pol->base.total_timeout = timeout;
            }
            pol->base.max_retries = max_retries_;
            pol->base.sleep_between_retries = sleep_between_retries_;
            pol->durable_delete = durable_delete_;
            pol->replica = replica_;

            pol->commit_level = commit_level_;

        }

        void init_batch_policy(uint32_t totalTimeoutMillis, uint32_t actual_size, as_policy_batch* pol) {

            as_policy_batch_init(pol);

            if (totalTimeoutMillis > 0) {
                pol->base.total_timeout = totalTimeoutMillis;
            }

            pol->base.max_retries = max_retries_;
            pol->base.sleep_between_retries = sleep_between_retries_;
            pol->consistency_level = consistency_level_;

            pol->send_set_name = true;

            if (actual_size >= min_concurrent_batch_size_) {
                pol->concurrent = true;
            }

        }

        void init_scan_policy(as_policy_scan* pol) {

            as_policy_scan_init(pol);

            pol->base.total_timeout = 0;
            pol->base.max_retries = max_retries_;
            pol->base.sleep_between_retries = sleep_between_retries_;
            pol->fail_on_cluster_change = false;

        }

        void init_query_policy(as_policy_query* pol) {

            as_policy_query_init(pol);

            pol->base.total_timeout = 0;
            pol->base.max_retries = max_retries_;
            pol->base.sleep_between_retries = sleep_between_retries_;
            pol->deserialize = false;

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

        bool init_key(const Key &requestKey, const std::string& table_override, as_key& key, std::shared_ptr<AerospikeSet>& set, StatusErr& statusErr) {

            if (table_override.empty()) {
                statusErr.bad_request("empty table name");
                return false;
            }

            auto i = sets_.find(table_override);
            if (i == sets_.end()) {
                statusErr.resource("table not found");
                return false;
            }

            set = i->second;

            switch(requestKey.recordKey_case()) {

                case Key::RecordKeyCase::kRaw: {
                    const uint8_t* value = (const uint8_t*) requestKey.raw().c_str();
                    uint32_t len = static_cast<uint32_t>(requestKey.raw().length());
                    if (!as_key_init_rawp(&key, set->get_namespace().c_str(), set->get_set().c_str(), value, len, false)) {
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
                    if (!as_key_init_digest(&key, set->get_namespace().c_str(), set->get_set().c_str(), value)) {
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

            meta->add_version(rec->gen);
            meta->set_ttl(rec->ttl);

        }


        void key_result(as_key* key, ValueResult *result, const OutputOptions &out) {

            if (!check::include_key(out)) {
                return;
            }

            Key* res = result->mutable_key();
            res->set_tablename(key->set);

            bool setup = false;

            if (key->valuep) {

                as_value_ser binarer;
                binarer.set(key->valuep);

                if (binarer.has()) {
                    res->set_raw(binarer.data(), binarer.size());
                    setup = true;
                }

            }

            if (!setup) {

                as_digest *digest = as_key_digest(key);
                if (digest) {
                    res->set_digest(key->digest.value, AS_DIGEST_VALUE_SIZE);
                }

            }

        }

        void value_result(as_record *rec, ValueResult *result, const OutputOptions &out, bool single_bin) {

            if (check::include_value(out)) {
                as_record_ser ser;

                size_t pos = ser.pack_record(rec, single_bin);

                result->mutable_value()->set_raw(ser.data(), ser.size());
            }

        }


        /**
         * MULTI_GET
         */

        struct multiGet_context {

            AerospikeDriver* driver;
            const std::vector<MultiGetEntry>& entries;
            std::vector<std::shared_ptr<AerospikeSet>> sets;
            std::unordered_map<const as_key*, int, as_key_hash, as_key_equal> key_map;

        };

        void do_multi_get(const std::vector<MultiGetEntry>& entries);

        bool multiGet_callback(const as_batch_read* results, uint32_t n, multiGet_context* context);

        static bool static_multiGet_callback(const as_batch_read* results, uint32_t n, void* udata);


        /**
         * SCAN
         */

        struct scan_context {

            AerospikeDriver* instance;
            grpc::ServerWriter<::gkvs::ValueResult> *writer;
            const ScanOperation* operation;
            bool single_bin;
            mutable std::mutex scan_mutex;

        };

        void do_scan(const ScanOperation *request, const std::string& table_override, ::grpc::ServerWriter<ValueResult> *writer);

        bool static static_scan_callback(const as_val* val, void* udata);

        bool scan_callback(const as_val* val, scan_context* context);

        /**
         * SIMPLE
         */


        void do_get(const KeyOperation *request, const std::string& table_override, ValueResult *response);

        void do_put(const PutOperation *request, const std::string& table_override, StatusResult *response);

        void do_remove(const KeyOperation *request, const std::string& table_override, StatusResult *response);


    };

    Driver* create_aerospike_driver(const std::string &name, const json& conf, const std::string &lua_dir) {
        return new AerospikeDriver(name, conf, lua_dir);
    }


}

static bool glog_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{

    va_list ap;
    va_start(ap, fmt);
    // allocate in heap to avoid stack overflow by untrusted vsnprintf function
    char str[AS_MAX_LOG_STR];
    int affected = vsnprintf(str, AS_MAX_LOG_STR, fmt, ap);
    if (affected > 0) {

        switch(level) {

            case AS_LOG_LEVEL_ERROR:
                LOG(ERROR) << std::this_thread::get_id() << ":" << str << std::endl;
                break;

            case AS_LOG_LEVEL_WARN:
                LOG(WARNING) << std::this_thread::get_id() << ":" << str << std::endl;
                break;

            case AS_LOG_LEVEL_INFO:
                LOG(INFO) << std::this_thread::get_id() << ":" << str << std::endl;
                break;

            case AS_LOG_LEVEL_DEBUG:
                DLOG(INFO) << std::this_thread::get_id() << ":" << str << std::endl;
                break;

            case AS_LOG_LEVEL_TRACE:
                VLOG(0) << std::this_thread::get_id() << ":" << str << std::endl;
                break;

            default:
                LOG(ERROR) << std::this_thread::get_id() << ":" << "unknown log level: " << level << ", msg: " << str << std::endl;
                break;

        }

    }
    va_end(ap);
    return true;
}


static gkvs::StatusCode parse_aerospike_status(as_status status) {

    switch(status) {

        case AEROSPIKE_OK:
            return gkvs::StatusCode::SUCCESS;

        case AEROSPIKE_NO_MORE_RECORDS:
        case AEROSPIKE_QUERY_END:
            return gkvs::StatusCode::ERROR_END_CQ;

        case AEROSPIKE_ERR_RECORD_GENERATION:
            return gkvs::StatusCode::SUCCESS_NOT_UPDATED;

        case AEROSPIKE_ERR_NAMESPACE_NOT_FOUND:
            return gkvs::StatusCode::ERROR_RESOURCE;

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
            return gkvs::StatusCode::ERROR_BAD_REQUEST;

        case AEROSPIKE_ERR_BIN_EXISTS:
        case AEROSPIKE_ERR_RECORD_EXISTS:
        case AEROSPIKE_ERR_RECORD_NOT_FOUND:
        case AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS:
        case AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND:
            return gkvs::StatusCode::ERROR_POLICY;

        case AEROSPIKE_ERR_CLUSTER_CHANGE:
            return gkvs::StatusCode::ERROR_MIGRATION;

        case AEROSPIKE_ERR_CLUSTER:
        case AEROSPIKE_ERR_INVALID_HOST:
        case AEROSPIKE_ERR_INVALID_NODE:
        case AEROSPIKE_ERR_NO_MORE_CONNECTIONS:
        case AEROSPIKE_ERR_ASYNC_CONNECTION:
        case AEROSPIKE_ERR_CONNECTION:
            return gkvs::StatusCode::ERROR_NETWORK;

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
            return gkvs::StatusCode::ERROR_AUTH;

        case AEROSPIKE_ERR_FAIL_FORBIDDEN:
        case AEROSPIKE_ERR_ALWAYS_FORBIDDEN:
        case AEROSPIKE_ERR_TLS_ERROR:
        case AEROSPIKE_NOT_AUTHENTICATED:
        case AEROSPIKE_ROLE_VIOLATION:
            return gkvs::StatusCode::ERROR_FORBIDDEN;

        case AEROSPIKE_ERR_QUERY_TIMEOUT:
        case AEROSPIKE_ERR_TIMEOUT:
            return gkvs::StatusCode::ERROR_TIMEOUT;

        case AEROSPIKE_ERR_BATCH_QUEUES_FULL:
        case AEROSPIKE_ERR_DEVICE_OVERLOAD:
        case AEROSPIKE_ERR_ASYNC_QUEUE_FULL:
        case AEROSPIKE_ERR_QUERY_QUEUE_FULL:
        case AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED:
            return gkvs::StatusCode::ERROR_OVERLOAD;

        case AEROSPIKE_ERR_SERVER_FULL:
        case AEROSPIKE_ERR_INDEX_OOM:
            return gkvs::StatusCode::ERROR_OVERFLOW;

        case AEROSPIKE_ERR_RECORD_BUSY:
            return gkvs::StatusCode::ERROR_LOCKED;

        case AEROSPIKE_ERR_SCAN_ABORTED:
        case AEROSPIKE_ERR_CLIENT_ABORT:
        case AEROSPIKE_ERR_QUERY_ABORTED:
            return gkvs::StatusCode::ERROR_ABORTED;

        case AEROSPIKE_ERR_UNSUPPORTED_FEATURE:
        case AEROSPIKE_ERR_BATCH_DISABLED:
            return gkvs::StatusCode::ERROR_UNSUPPORTED;

        case AEROSPIKE_ERR_CLIENT:
        case AEROSPIKE_ERR_SERVER:
        case AEROSPIKE_ERR_INDEX_NOT_READABLE:
        case AEROSPIKE_ERR_INDEX:
        case AEROSPIKE_ERR_QUERY:
        case AEROSPIKE_ERR_UDF:
            return gkvs::StatusCode::ERROR_DRIVER;

        default:
            return gkvs::StatusCode::ERROR_INTERNAL;


    }


}



void gkvs::AerospikeDriver::do_multi_get(const std::vector<MultiGetEntry>& entries) {

    multiGet_context context = {this, entries};

    uint32_t size = static_cast<uint32_t>(entries.size());

    as_batch batch;
    as_batch_init(&batch, size);

    bool includeValue = false;
    bool useSelect = true;
    std::set<std::string> select;

    int max_timeout = 0;
    uint32_t actual_size = 0;

    for (uint32_t i = 0; i < size; ++i) {

        context.sets.push_back(std::shared_ptr<AerospikeSet>());

        const MultiGetEntry& entry = entries[i];
        const KeyOperation &operation = entry.get_request();

        if (operation.has_header() && operation.header().timeout() > max_timeout) {
            max_timeout = operation.header().timeout();
        }

        StatusErr statusErr;
        as_key *key = as_batch_keyat(&batch, actual_size);
        if (!init_key(operation.key(), entry.get_table_override(), *key, context.sets[i], statusErr)) {
            statusErr.to_status(entry.get_response()->mutable_status());
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

        includeValue |= check::include_value(operation.output());
        context.key_map[key] = i;
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
            aerospike_batch_get_bins(&as_, &err, &pol, &batch, bins, n_bins, static_multiGet_callback, &context);
            delete[] bins;
        }
        else {
            aerospike_batch_get(&as_, &err, &pol, &batch, static_multiGet_callback, &context);
        }
    }
    else {
        aerospike_batch_exists(&as_, &err, &pol, &batch, static_multiGet_callback, &context);
    }

    as_batch_destroy(&batch);

}



bool gkvs::AerospikeDriver::static_multiGet_callback(const as_batch_read* results, uint32_t n, void* udata) {

    multiGet_context* context = (multiGet_context*) udata;
    return context->driver->multiGet_callback(results, n, context);
}


bool gkvs::AerospikeDriver::multiGet_callback(const as_batch_read* results, uint32_t n, multiGet_context* context) {

    const std::vector<MultiGetEntry>& entries = context->entries;

    for (uint32_t i = 0; i < n; ++i) {

        const as_key* key = results[i].key;
        if (!key) {
            LOG(ERROR) << "empty key in the batch: " << i << std::endl;
            continue;
        }

        auto k = context->key_map.find(key);
        if (k == context->key_map.end()) {
            char* pstr = as_val_tostring(key->valuep);
            if (pstr) {
                LOG(ERROR) << "operation not found for key: " << pstr << std::endl;
                cf_free(pstr);
            }
            continue;
        }

        int pos = k->second;
        const MultiGetEntry& entry = context->entries[pos];
        const std::shared_ptr<AerospikeSet> set = context->sets[pos];
        const KeyOperation& operation =  entry.get_request();
        ValueResult *result = entry.get_response();

        as_status status = results[i].result;

        if (status == AEROSPIKE_OK) {

            const as_record *rec = &results[i].record;

            metadata_result(const_cast<as_record *>(rec), result);
            value_result(const_cast<as_record *>(rec), result, operation.output(), set->is_single_bin());

            status::success(result->mutable_status());

        }
        else if (status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {

            // return no metadata, that means record was not found

            status::success(result->mutable_status());
        }
        else {
            error(results[i].result, result->mutable_status());
        }

    }

    return true;
}


void gkvs::AerospikeDriver::do_scan(const ::gkvs::ScanOperation *request, const std::string& table_override, ::grpc::ServerWriter<::gkvs::ValueResult> *writer) {

    auto i = sets_.find(table_override);
    if (i == sets_.end()) {
        ValueResult result;
        result::header(request->header(), result.mutable_header());
        status::error_resource("set not found", result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
        return;
    }

    std::shared_ptr<AerospikeSet> set = i->second;

    scan_context context { this, writer, request, set->is_single_bin() };

    as_scan scan;
    as_scan_init(&scan, set->get_namespace().c_str(), set->get_set().c_str());
    scan.deserialize_list_map = true;

    if (!check::include_value(request->output())) {
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
    init_scan_policy(&pol);

    as_error err;
    as_status status = aerospike_scan_foreach(&as_, &err, &pol, &scan, static_scan_callback, &context);

    if (status != AEROSPIKE_OK) {
        ValueResult result;
        result::header(request->header(), result.mutable_header());
        error(err, result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
    }

    as_scan_destroy(&scan);

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

        result::header(operation->header(), result.mutable_header());
        status::success(result.mutable_status());
        metadata_result(rec, &result);
        key_result(&rec->key, &result, operation->output());
        value_result(rec, &result, operation->output(), context->single_bin);


        std::lock_guard< std::mutex > guard( context->scan_mutex );
        writer->Write(result, grpc::WriteOptions());
    }

    return true;
}


void gkvs::AerospikeDriver::do_get(const ::gkvs::KeyOperation *request, const std::string& table_override, ::gkvs::ValueResult *response) {

    std::shared_ptr<AerospikeSet> set;
    StatusErr statusErr;
    as_key key;
    if (!init_key(request->key(), table_override, key, set, statusErr)) {
        statusErr.to_status(response->mutable_status());
        return;
    }

    uint32_t timeout = request->has_header() ? request->header().timeout() : set->get_timeout();

    as_policy_read pol;
    init_read_policy(timeout, &pol);

    as_record* rec = nullptr;
    as_error err;
    as_status status;

    if (check::include_value(request->output())) {
        if (request->has_select()) {
            const char** bins = allocate_bins(request->select());
            status = aerospike_key_select(&as_, &err, &pol, &key, bins, &rec);
            delete [] bins;
        }
        else {
            status = aerospike_key_get(&as_, &err, &pol, &key, &rec);
        }
    }
    else {
        status = aerospike_key_exists(&as_, &err, &pol, &key, &rec);
    }

    if (status == AEROSPIKE_OK) {

        if (rec) {

            metadata_result(rec, response);
            value_result(rec, response, request->output(), set->is_single_bin());

        }

        status::success(response->mutable_status());

    }
    else if (status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {

        // return no record, means no record was found, this is not an error, it is like a map interface for records
        // not like database interface

        status::success(response->mutable_status());
    }
    else {
        error(err, response->mutable_status());
    }

    if (rec != nullptr) {
        as_record_destroy(rec);
    }


}

void gkvs::AerospikeDriver::do_put(const ::gkvs::PutOperation *request, const std::string& table_override, ::gkvs::StatusResult *response) {

    std::shared_ptr<AerospikeSet> set;
    StatusErr statusErr;
    as_key key;

    if (!init_key(request->key(), table_override, key, set, statusErr)) {
        statusErr.to_status(response->mutable_status());
        return;
    }

    const Value& val = request->value();

    gkvs::as_record_ser record_ser;
    as_record* rec = record_ser.unpack_record(val.raw(), set->is_single_bin());

    if (0 == as_record_numbins(rec)) {
        status::success_not_updated(response->mutable_status());
        return;
    }

    uint32_t timeout = request->has_header() ? request->header().timeout() : 0;

    as_policy_write pol;
    init_write_policy(timeout, &pol);

    int ttl = request->ttl();
    if (ttl == 0) {
        ttl = set->get_ttl();
    }
    if (ttl > 0) {
        rec->ttl = static_cast<uint32_t>(ttl);
    }

    // save the key if sent
    if (request->key().recordKey_case() == Key::RecordKeyCase::kRaw) {
        pol.key = AS_POLICY_KEY_SEND;
    }

    switch(request->policy()) {
        case REPLACE:
            pol.exists = AS_POLICY_EXISTS_CREATE_OR_REPLACE;
            break;
        case MERGE:
        default:
            pol.exists = AS_POLICY_EXISTS_IGNORE;
            break;
    }

    // set version for CompareAndPut
    if (request->compareandput()) {
        pol.gen = AS_POLICY_GEN_EQ;
        if (request->version_size() >= 1) {
            rec->gen = static_cast<uint16_t>(request->version(0));
        }
        else {
            rec->gen = 0;
        }
    }
    else {
        pol.gen = AS_POLICY_GEN_IGNORE;
    }

    as_error err;
    as_status status = aerospike_key_put(&as_, &err, &pol, &key, rec);

    if (status == AEROSPIKE_OK) {

        status::success(response->mutable_status());

    }
    else if (status == AEROSPIKE_ERR_RECORD_GENERATION && request->compareandput()) {

        response->mutable_status()->set_code(StatusCode::SUCCESS_NOT_UPDATED);

    }
    else {
        error(err, response->mutable_status());
    }

    //as_record_destroy(rec);
}

void gkvs::AerospikeDriver::do_remove(const ::gkvs::KeyOperation *request, const std::string& table_override, ::gkvs::StatusResult *response) {

    std::shared_ptr<AerospikeSet> set;
    StatusErr statusErr;
    as_key key;
    if (!init_key(request->key(), table_override, key, set, statusErr)) {
        statusErr.to_status(response->mutable_status());
        return;
    }

    uint32_t timeout = request->has_header() ? request->header().timeout() : set->get_timeout();

    as_error err;
    as_status status;

    if (request->has_select()) {

        as_policy_write pol;
        init_write_policy(timeout, &pol);

        const Select& select = request->select();

        auto size = static_cast<uint16_t>(select.column().size());

        as_record* rec = as_record_new(size);

        for (const std::string &col : select.column()) {
            as_record_set_nil(rec, col.c_str());
        }

        status = aerospike_key_put(&as_, &err, &pol, &key, rec);

        as_record_destroy(rec);

    }
    else {

        // remove the whole record

        as_policy_remove pol;
        init_remove_policy(timeout, &pol);

        status = aerospike_key_remove(&as_, &err, &pol, &key);

    }

    if (status == AEROSPIKE_OK) {

        status::success(response->mutable_status());

    }
    else if (status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {

        // this is not an error

        status::success_not_updated(response->mutable_status());
    }
    else {
        error(err, response->mutable_status());
    }

}