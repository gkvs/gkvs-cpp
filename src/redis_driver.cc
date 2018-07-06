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

#include "driver.h"
#include "crypto.h"
#include "redis_driver.h"

#include <glog/logging.h>

#include <hiredis/hiredis.h>
#include <msgpack.h>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace gkvs {


    class RedisDriver final : public Driver {

    public:

        explicit RedisDriver(const std::string& name, const json &conf) : Driver(name) {

            auto host_it = conf.find("host");
            auto port_it = conf.find("port");

            hostname_ = host_it != conf.end() ? host_it->get<std::string>() : "127.0.0.1";
            port_ = port_it != conf.end() ? port_it->get<int>() : 6379;

            LOG(INFO) << "Redis connect to " << hostname_ << ":" << port_ << std::endl;

            context_ = redisConnect(hostname_.c_str(), port_);

            if (context_ == nullptr || context_->err) {
                if (context_) {
                    std::cout << "Connection error: " << context_->errstr << std::endl;
                    redisFree(context_);
                } else {
                    std::cout << "Connection error: can't allocate redis context" << std::endl;
                }
            }

            redisEnableKeepAlive(context_);

        }

        ~RedisDriver() override {

            redisFree(context_);

            std::cout << "Graceful shutdown redis connection" << std::endl;

        }


        void get(const KeyOperation *request, ValueResult *response) override {

            do_get(request, response);

        }

        void multiGet(const BatchKeyOperation *request, BatchValueResult *response) override {

            do_multi_get(request, response);

        }

        void scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer) override {

            do_scan(request, writer);

        }

        void put(const PutOperation *request, StatusResult *response) override {

            do_put(request, response);

        }

        void remove(const KeyOperation *request, StatusResult *response) override {

            do_remove(request, response);

        }


    private:

        redisContext *context_;

        std::string hostname_;
        int port_;


    protected:

        void do_multi_get(const BatchKeyOperation *request, BatchValueResult *response);

        void do_scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer);

        bool do_scan(const ScanOperation *request, char* offset, int offset_size, int limit, int* affected, ::grpc::ServerWriter<ValueResult> *writer);

        void do_get(const KeyOperation *request, ValueResult *response);

        void do_put(const PutOperation *request, StatusResult *response);

        void do_remove(const KeyOperation *request, StatusResult *response);

        void error(redisReply *reply, Status *status) {
            status->set_code(StatusCode::ERROR_DRIVER);
            status->set_errorcode(reply->type);
            status->set_errormessage(reply->str);
        }

        void key_result(const Key& key, ValueResult *result, const OutputOptions &out) {

            if (include_key(out)) {
                result->mutable_key()->CopyFrom(key);
            }

        }

        void metadata_result(const uint64_t* verOrNull, int ttl, ValueResult *result) {

            Metadata *meta = result->mutable_metadata();

            if (verOrNull != nullptr) {
                set_version(verOrNull, meta);
            }

            meta->set_ttl(ttl);

        }

        void set_version(const uint64_t* ver, Metadata *meta) {

            meta->add_version(ver[0] & 0xFFFFFFFF);
            meta->add_version(ver[0] >> 32);

            meta->add_version(ver[1] & 0xFFFFFFFF);
            meta->add_version(ver[1] >> 32);

        }

        bool get_version(const PutOperation* request, uint64_t* rver) {

            if (request->version_size() == 4) {
                rver[0] = (((uint64_t)request->version(1)) << 32) | request->version(0);
                rver[1] = (((uint64_t)request->version(3)) << 32) | request->version(2);
                return true;
            }

            return false;
        }

        void key_result(const redis_reply& reply, gkvs::Key* key, const OutputOptions &out);

        std::string value_result(const redis_reply& reply, gkvs::Value *value, const OutputOptions &out);

    };


    Driver* create_redis_driver(const std::string &name, const json& conf) {
        return new RedisDriver(name, conf);
    }


}

const redisReply gkvs::redis_reply::null_reply_ = { REDIS_REPLY_ERROR, 0, 0, (char*) "redis error null", 0, {} };

void gkvs::redis_reply::pack_redis_value(msgpack_packer& pk, const redisReply *reply) {

    switch(reply->type) {

        case REDIS_REPLY_NIL:
            msgpack_pack_nil(&pk);
            break;

        case REDIS_REPLY_STRING: {
            size_t len = strlen(reply->str);
            msgpack_pack_str(&pk, len);
            msgpack_pack_str_body(&pk, reply->str, len);
            break;
        }

        case REDIS_REPLY_INTEGER:
            msgpack_pack_int64(&pk, reply->integer);
            break;

        case REDIS_REPLY_ARRAY: {
            int size = reply->elements;
            msgpack_pack_array(&pk, size);
            for (int i = 0; i < size; ++i) {
                pack_redis_value(pk, reply->element[i]);
            }
            break;
        }

        default:
            msgpack_pack_nil(&pk);
            break;

    }

}

std::string gkvs::redis_reply::pack_redis_value(const redisReply *reply) {

    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    pack_redis_value(pk, reply);

    std::string result(sbuf.data, sbuf.size);
    msgpack_sbuffer_destroy(&sbuf);
    return result;
}

void gkvs::RedisDriver::key_result(const redis_reply& reply, gkvs::Key* key, const OutputOptions &out) {

    bool includeKey = include_key(out);

    if (includeKey) {

        std::string raw = reply.to_raw();

        size_t delim = raw.find(':', 0);
        if (delim == -1 || delim + 1 >= raw.size()) {
            key->set_raw(raw);
        }
        else {
            key->set_tablename(raw.substr(0, delim));
            size_t pos = delim + 1;
            key->set_raw(raw.substr(pos, raw.size() - pos));
        }

    }

}

std::string gkvs::RedisDriver::value_result(const redis_reply& reply, gkvs::Value *value, const OutputOptions &out) {

     std::string raw = reply.to_raw();

     if (include_value(out)) {
         value->set_raw(raw);
     }

     return raw;

}

void gkvs::RedisDriver::do_multi_get(const BatchKeyOperation *request, BatchValueResult *response) {

    int size = request->operation_size();

    for (int i = 0; i < size; ++i) {

        ValueResult* result = response->add_result();
        do_get(&request->operation(i), result);

    }

}

void gkvs::RedisDriver::do_scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer) {

    const std::string &tableName = request->tablename();

    if (tableName.empty()) {
        ValueResult result;
        result.mutable_header()->set_tag(request->header().tag());
        bad_request("empty table name", result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
        return;
    }

    int limit = 1000;
    char offset[255];
    strcpy(offset, "0");
    while(true) {

        int affected = 0;

        if (!do_scan(request, offset, sizeof(offset), limit, &affected, writer)) {
            break;
        }

        if (strncmp("0", offset, sizeof(offset)) == 0) {
            break;
        }

        if (affected < 1000) {
            limit = limit * 2;
        }

    }

}

bool gkvs::RedisDriver::do_scan(const ScanOperation *request, char* offset, int offset_size, int limit, int *affected, ::grpc::ServerWriter<ValueResult> *writer) {

    *affected = 0;

    redis_reply scanReply;
    scanReply = (redisReply *) redisCommand(context_, "SCAN %s MATCH %s:* COUNT %i", offset, request->tablename().c_str(), limit);

    if (scanReply.empty()) {
        ValueResult result;
        result.mutable_header()->set_tag(request->header().tag());
        driver_error("redis error", result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
        return false;
    }

    if (scanReply.is_error()) {
        ValueResult result;
        result.mutable_header()->set_tag(request->header().tag());
        error(scanReply.get(), result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
        return false;
    }

    if (scanReply.type() == REDIS_REPLY_STATUS) {
        ValueResult result;
        result.mutable_header()->set_tag(request->header().tag());
        status_error(scanReply.type(), scanReply.str(), result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
        return false;
    }

    if (scanReply.type() != REDIS_REPLY_ARRAY) {
        ValueResult result;
        result.mutable_header()->set_tag(request->header().tag());
        driver_error(scanReply.type(), "wrong reply type", result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
        return false;
    }

    int scanArray = scanReply.elements();

    if (scanArray != 2) {
        ValueResult result;
        result.mutable_header()->set_tag(request->header().tag());
        driver_error(scanReply.type(), "wrong scan array", result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
        return false;
    }

    redisReply* nextOffsetElement = scanReply.element(0);
    if (nextOffsetElement->type != REDIS_REPLY_STRING) {
        ValueResult result;
        result.mutable_header()->set_tag(request->header().tag());;
        driver_error(nextOffsetElement->type, "wrong next offset type", result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
        return false;
    }

    strncpy(offset, nextOffsetElement->str, offset_size);

    redisReply* keysReply = scanReply.element(1);

    if (keysReply != nullptr && keysReply->type == REDIS_REPLY_ARRAY) {

        int size = keysReply->elements;

        for (int i = 0; i < size; ++i) {

            ValueResult response;
            response.mutable_header()->set_tag(request->header().tag());

            redisReply *element = keysReply->element[i];

            if (element == nullptr || element->type == REDIS_REPLY_ERROR) {
                error(element, response.mutable_status());
            } else {
                metadata_result(nullptr, -1, &response);
                key_result(redis_reply(element, false), response.mutable_key(), OutputOptions::KEY);
                success(response.mutable_status());
            }

            writer->Write(response, grpc::WriteOptions());
            (*affected)++;
        }
    }

    return true;
}

void gkvs::RedisDriver::do_get(const KeyOperation *request, ValueResult *response) {

    response->mutable_header()->set_tag(request->header().tag());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    // for client identification purpose
    key_result(request->key(), response, request->output());

    const std::string& tableName = request->key().tablename();

    if (tableName.empty()) {
        bad_request("empty table name", response->mutable_status());
        return;
    }

    if (request->key().recordKey_case() != Key::RecordKeyCase::kRaw) {
        bad_request("key must be raw", response->mutable_status());
        return;
    }

    std::string key = tableName + ":" + request->key().raw();

    redis_reply reply;

    if (request->output() == OutputOptions::METADATA) {

        reply = (redisReply *) redisCommand(context_, "TTL %b", key.c_str(), key.size());

        if (reply.empty()) {
            driver_error("redis error", response->mutable_status());
            return;
        }

        if (!reply.is_number()) {
            error(reply.get(), response->mutable_status());
        }
        else if (reply.number() == -2) {
            // not exists
            success(response->mutable_status());
        }
        else if (reply.number() == -1) {
            // exists with no TTL
            metadata_result(nullptr, -1, response);
            success(response->mutable_status());
        }
        else {
            metadata_result(nullptr, reply.number(), response);
            success(response->mutable_status());
        }

    }
    else {
        reply = (redisReply *) redisCommand(context_, "GET %b", key.c_str(), key.size());

        if (reply.empty()) {
            driver_error("redis error", response->mutable_status());
            return;
        }

        if (reply.is_error()) {
            error(reply.get(), response->mutable_status());
        }
        else if (reply.type() == REDIS_REPLY_NIL) {
            // not found
            success(response->mutable_status());
        }
        else {
            std::string raw = value_result(reply, response->mutable_value(), request->output());

            MurMur3 murmur3;
            uint64_t* cv = murmur3.hash128(raw);

            metadata_result(cv, -1, response);

            success(response->mutable_status());
        }

    }

}

void gkvs::RedisDriver::do_put(const PutOperation *request, StatusResult *response) {

    response->mutable_header()->set_tag(request->header().tag());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    if (request->key().recordKey_case() != Key::RecordKeyCase::kRaw) {
        bad_request("key must be raw", response->mutable_status());
        return;
    }

    const std::string& tableName = request->key().tablename();

    if (tableName.empty()) {
        bad_request("empty table name", response->mutable_status());
        return;
    }

    std::string key = tableName + ":" + request->key().raw();

    if (!request->has_value()) {
        bad_request("no value", response->mutable_status());
        return;
    }

    const std::string& value = request->value().raw();

    bool updated = true;
    redis_reply reply;

    int ttl = request->ttl();
    if (request->compareandput()) {

        if (request->version_size() == 0) {

            /**
             * PutIfAbsent
             */

            reply = (redisReply *) redisCommand(context_, "SETNX %b %b", key.c_str(), key.size(), value.c_str(), value.size());

            if (reply.empty()) {
                driver_error("redis error", response->mutable_status());
                return;
            }

            if (!reply.is_number()) {
                driver_error("expected number for SETNX", response->mutable_status());
                return;
            }

            updated = reply.number() == 1;

            if (updated && ttl > 0) {
                reply = (redisReply *) redisCommand(context_, "EXPIRE %b %i", key.c_str(), key.size(), ttl);
            }

        }
        else {

            /**
             * Replace
             */

            reply = (redisReply *) redisCommand(context_, "GET %b", key.c_str(), key.size());

            if (reply.empty()) {
                driver_error("redis error", response->mutable_status());
                return;
            }

            std::string raw = reply.to_raw();

            MurMur3 murmur3;
            uint64_t* cv = murmur3.hash128(raw);

            uint64_t rv[2] = {0, 0};
            bool got_ver = get_version(request, rv);

            if (!got_ver || cv[0] != rv[0] || cv[1] != rv[1]) {
                updated = false;
            }
            else {

                if (ttl > 0) {
                    reply = (redisReply *) redisCommand(context_, "SETEX %b %i %b", key.c_str(), key.size(), ttl, value.c_str(), value.size());
                }
                else {
                    reply = (redisReply *) redisCommand(context_, "SET %b %b", key.c_str(), key.size(), value.c_str(), value.size());
                }

            }

        }

    }
    else if (ttl > 0) {

        /**
         * PutWithTTL
         */

        reply = (redisReply *) redisCommand(context_, "SETEX %b %i %b", key.c_str(), key.size(), ttl, value.c_str(), value.size());
    }
    else {

        /**
         * Put
         */

        reply = (redisReply *) redisCommand(context_, "SET %b %b", key.c_str(), key.size(), value.c_str(), value.size());
    }

    if (reply.empty()) {
        driver_error("redis error", response->mutable_status());
        return;
    }

    if (reply.is_error()) {
        error(reply.get(), response->mutable_status());
    }
    else if (updated) {
        success(response->mutable_status());
    }
    else {
        success_not_updated(response->mutable_status());
    }


}

void gkvs::RedisDriver::do_remove(const KeyOperation *request, StatusResult *response) {

    response->mutable_header()->set_tag(request->header().tag());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    if (request->key().recordKey_case() != Key::RecordKeyCase::kRaw) {
        bad_request("key must be raw", response->mutable_status());
        return;
    }

    const std::string& tableName = request->key().tablename();

    if (tableName.empty()) {
        bad_request("empty table name", response->mutable_status());
        return;
    }

    std::string key = tableName + ":" + request->key().raw();

    redis_reply reply;
    reply = (redisReply *) redisCommand(context_, "DEL %b", key.c_str(), key.size());

    if (reply.empty()) {
        driver_error("redis error", response->mutable_status());
        return;
    }

    if (reply.is_error()) {
        error(reply.get(), response->mutable_status());
    }
    else if (reply.is_number() && reply.number() > 0) {
        success(response->mutable_status());
    }
    else {
        success_not_updated(response->mutable_status());
    }


}