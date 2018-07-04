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

            hostname_ = conf["host"].get<std::string>();
            port_ = conf["port"].get<int>();

            LOG(INFO) << "Redis connect to " << hostname_ << ":" << port_ << std::endl;

            context_ = redisConnectNonBlock(hostname_.c_str(), port_);

            if (context_ == nullptr || context_->err) {
                if (context_) {
                    std::cout << "Connection error: " << context_->errstr << std::endl;
                    redisFree(context_);
                    throw std::runtime_error("connection error");
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

        void do_get(const KeyOperation *request, ValueResult *response);

        void do_put(const PutOperation *request, StatusResult *response);

        void do_remove(const KeyOperation *request, StatusResult *response);

        void error(redisReply *reply, Status *status) {
            status->set_code(StatusCode::ERROR_DRIVER);
            status->set_errorcode(reply->type);
            status->set_errormessage(reply->str);
        }

        void value_result(redisReply *reply, gkvs::Value *value, const OutputOptions &out);

        inline bool is_error(redisReply *reply) {
            return reply->type == REDIS_REPLY_ERROR;
        }

        inline std::string parse_redis_value(redisReply *reply);

    };


    Driver* create_redis_driver(const std::string &name, const json& conf) {
        return new RedisDriver(name, conf);
    }


}

inline std::string gkvs::RedisDriver::parse_redis_value(redisReply *reply) {

    if (reply->type == REDIS_REPLY_ARRAY) {
        return std::string(reply->str, reply->len);
    }

    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

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

        case REDIS_REPLY_ARRAY:
            break;

        default:
            msgpack_pack_nil(&pk);
            break;

    }

    std::string result(sbuf.data, sbuf.size);
    msgpack_sbuffer_destroy(&sbuf);
    return result;
}

 void gkvs::RedisDriver::value_result(redisReply *reply, gkvs::Value *value, const OutputOptions &out) {

     bool includeValue = include_value(out);

     if (!includeValue) {
         return;
     }

     bool includeValueDigest = include_value_digest(out);

     std::string ser = parse_redis_value(reply);

     if (includeValueDigest) {
         Ripend160Hash hash;
         hash.apply(ser.data(), ser.size());
         value->set_raw(hash.data(), hash.size());
     }
     else {
         value->set_raw(ser.data(), ser.size());
     }

}

void gkvs::RedisDriver::do_multi_get(const BatchKeyOperation *request, BatchValueResult *response) {

    int size = request->operation_size();

    if (size == 0) {
        return;
    }

    for (int i = 0; i < size; ++i) {

        ValueResult* result = response->add_result();
        do_get(&request->operation(i), result);

    }

}

void gkvs::RedisDriver::do_scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer) {

    redisReply *keysReply = (redisReply *) redisCommand(context_, "KEYS *");

    if (!keysReply) {
        ValueResult response;
        driver_error("redis error", response.mutable_status());
        writer->WriteLast(response, grpc::WriteOptions());
        return;
    }

    if (is_error(keysReply)) {
        ValueResult response;
        error(keysReply, response.mutable_status());
        writer->WriteLast(response, grpc::WriteOptions());
        return;
    }

    if (keysReply->type != REDIS_REPLY_ARRAY) {
        ValueResult response;
        driver_error(keysReply->type, "wrong reply type", response.mutable_status());
        writer->WriteLast(response, grpc::WriteOptions());
        return;
    }

    grpc::WriteOptions writeOptions = grpc::WriteOptions();

    int size = keysReply->elements;
    for(int i = 0; i < size; ++i) {

        ValueResult response;

        redisReply * element = keysReply->element[i];

        if (is_error(element)) {
            error(element, response.mutable_status());
        }
        else {
            value_result(element, response.mutable_value(), request->output());
            success(response.mutable_status());
        }

        if (i + 1 == size) {
            writeOptions = writeOptions.set_last_message();
        }

        writer->Write(response, writeOptions);

    }

    freeReplyObject(keysReply);


}

void gkvs::RedisDriver::do_get(const KeyOperation *request, ValueResult *response) {

    response->set_requestid(request->options().requestid());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    if (request->key().recordKey_case() != Key::RecordKeyCase::kRaw) {
        bad_request("key must be raw", response->mutable_status());
        return;
    }

    const std::string& key = request->key().raw();

    redisReply *reply = nullptr;

    reply = (redisReply *) redisCommand(context_, "GET %b", key.c_str(), key.size());

    if (!reply) {
        driver_error("redis error", response->mutable_status());
        return;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        error(reply, response->mutable_status());
    }
    else {

        value_result(reply, response->mutable_value(), request->output());

        success(response->mutable_status());
    }

    freeReplyObject(reply);

}

void gkvs::RedisDriver::do_put(const PutOperation *request, StatusResult *response) {

    response->set_requestid(request->options().requestid());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    if (request->key().recordKey_case() != Key::RecordKeyCase::kRaw) {
        bad_request("key must be raw", response->mutable_status());
        return;
    }

    const std::string& key = request->key().raw();

    if (!request->has_value()) {
        bad_request("no value", response->mutable_status());
        return;
    }

    if (request->value().value_case() != Value::ValueCase::kRaw) {
        bad_request("value must be raw", response->mutable_status());
        return;
    }

    const std::string& value = request->value().raw();

    redisReply *reply = nullptr;

    reply = (redisReply *) redisCommand(context_, "SET %b %b", key.c_str(), key.size(), value.c_str(), value.size());

    if (!reply) {
        driver_error("redis error", response->mutable_status());
        return;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        error(reply, response->mutable_status());
    }
    else {
        success(response->mutable_status());
    }

    freeReplyObject(reply);

}

void gkvs::RedisDriver::do_remove(const KeyOperation *request, StatusResult *response) {

    response->set_requestid(request->options().requestid());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    if (request->key().recordKey_case() != Key::RecordKeyCase::kRaw) {
        bad_request("key must be raw", response->mutable_status());
        return;
    }

    const std::string& key = request->key().raw();

    redisReply *reply = nullptr;

    reply = (redisReply *) redisCommand(context_, "DEL %b", key.c_str(), key.size());

    if (!reply) {
        driver_error("redis error", response->mutable_status());
        return;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        error(reply, response->mutable_status());
    }
    else {
        success(response->mutable_status());
    }

    freeReplyObject(reply);

}