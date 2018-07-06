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

#include <hiredis/hiredis.h>
#include <msgpack.h>

namespace gkvs {


    class redis_reply {

    public:

        explicit redis_reply() {
            reply_ = null_reply();
            free_ = false;
        }

        explicit redis_reply(redisReply *reply) {
            reply_ = reply;
            free_ = true;
        }

        explicit redis_reply(redisReply *reply, bool free) {
            reply_ = reply;
            free_ = free;
        }

        void operator=(redisReply *reply) {
            if (free_) {
                freeReplyObject(reply_);
            }
            if (reply == nullptr) {
                reply_ = null_reply();
                free_ = false;
            }
            else {
                reply_ = reply;
                free_ = true;
            }
        }

        ~redis_reply() {
            if (free_) {
                freeReplyObject(reply_);
            }
        }

        inline long long number() {
            return reply_->integer;
        }

        inline char* str() {
            return reply_->str;
        }

        inline int elements() {
            return reply_->elements;
        }

        inline redisReply* element(int i) {
            return reply_->element[i];
        }

        inline int type() {
            return reply_->type;
        }

        inline bool is_error() {
            return reply_->type == REDIS_REPLY_ERROR;
        }

        inline bool is_number() {
            return reply_->type == REDIS_REPLY_INTEGER;
        }

        inline bool is_string() {
            return reply_->type == REDIS_REPLY_STRING;
        }

        inline bool is_status() {
            return reply_->type == REDIS_REPLY_STATUS;
        }

        inline bool empty() {
            return reply_ == null_reply();
        }

        inline redisReply* get() {
            return reply_;
        }

        inline std::string to_raw() const {

            switch(reply_->type) {

                case REDIS_REPLY_NIL:
                    return std::string();
                case REDIS_REPLY_STRING:
                    return std::string(reply_->str, reply_->len);
                case REDIS_REPLY_INTEGER:
                    return std::to_string(reply_->integer);

                case REDIS_REPLY_ARRAY:
                default:
                    return pack_redis_value(reply_);

            }

        }

        static void pack_redis_value(msgpack_packer& pk, const redisReply *reply);

        static std::string pack_redis_value(const redisReply *reply);

    private:

        redisReply *reply_;
        bool free_;

        static const redisReply null_reply_;

        redisReply* null_reply() {
            return (redisReply*) &null_reply_;
        }

    };

}