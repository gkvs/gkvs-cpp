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
#include "redis_driver.h"

#include <hiredis/hiredis.h>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace gkvs {


    class RedisDriver final : public Driver {

    public:

        explicit RedisDriver(const std::string &conf_str) : Driver() {

            json conf = nlohmann::json::parse(conf_str.begin(), conf_str.end());

            const char *hostname = conf["host"].get<std::string>().c_str();
            int port = conf["port"].get<int>();

            context_ = redisConnectNonBlock(hostname, port);

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

    };


    Driver* create_redis_driver(const std::string &conf_str, const std::string &lua_path) {
        return new RedisDriver(conf_str);
    }

}


void gkvs::RedisDriver::do_multi_get(const BatchKeyOperation *request, BatchValueResult *response) {



}

void gkvs::RedisDriver::do_scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer) {


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

        switch(reply->type) {

            case REDIS_REPLY_NIL:
                break;

            case REDIS_REPLY_STRING:
                break;

            case REDIS_REPLY_INTEGER:
                break;

            case REDIS_REPLY_ARRAY:
                break;

            case REDIS_REPLY_STATUS:
                break;

        }

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