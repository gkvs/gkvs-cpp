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

#include <aerospike/aerospike_key.h>
#include <aerospike/as_boolean.h>

#include <msgpack.h>
#include <iostream>

namespace gkvs {

    struct as_key_hash {

        size_t operator()(const as_key *key) const;

    };

    struct as_key_equal {

        bool operator()(const as_key *lhs, const as_key *rhs) const;

    };

    class as_record_ser final {

    public:

        explicit as_record_ser() {
        }

        ~as_record_ser() {
            if (rec_free_) {
                as_record_destroy(rec_);
            }
            if (sbuf_free_) {
                msgpack_sbuffer_destroy(&sbuf_);
            }
            if (mempool_free_) {
                msgpack_zone_destroy(&mempool_);
            }
        }

        as_record* get() {
            return rec_;
        }

        const char* data() {
            if (!sbuf_free_) {
                return nullptr;
            }
            return &sbuf_.data[sbuf_pos_];
        }

        size_t size() {
            if (!sbuf_free_) {
                return 0;
            }
            return sbuf_.size - sbuf_pos_;
        }

        as_record* unpack_record(const char* data, size_t size, bool single_bin);

        inline as_record* unpack_record(const std::string& mp, bool single_bin) {
            return unpack_record(mp.c_str(), mp.length(), single_bin);
        }

        bool pack_record(as_record *rec, bool single_bin);

    private:

        as_list* unpack_list(const msgpack_object &val_obj);

        as_map* unpack_map(const msgpack_object &val_obj);

        as_val* unpack_val(const msgpack_object &val_obj);

        void stringify(msgpack_object& obj, char* buf, uint32_t size);

        void record_set(const char* key, const msgpack_object& val_obj);

        void pack_list(as_list* list);

        void pack_map(as_map* map);

        void pack_val(as_val* value);

        void pack_bin_value(as_bin_value *value);

        as_record* alloc_rec(size_t size);

        as_record* rec_;
        bool rec_free_ = false;

        msgpack_packer pk_;

        msgpack_sbuffer sbuf_;
        bool sbuf_free_ = false;
        size_t sbuf_pos_ = 0;

        msgpack_zone mempool_;
        bool mempool_free_ = false;

    };


    class as_value_ser final {

    public:

        as_value_ser() {
            _buf = nullptr;
            _free = false;
        }

        ~as_value_ser() {
            if (_free && _buf != nullptr) {
                delete [] _buf;
            }
        }

        void reset() {
            if (_free && _buf != nullptr) {
                delete [] _buf;
            }
            _free = false;
            _buf = nullptr;
            _size = 0;
        }

        void set(as_key_value* value);

        void set(as_bin_value* value);

        void set(as_val* value);

        bool has() {
            return _buf != nullptr;
        }

        const uint8_t* data() {
            return _buf;
        }

        int32_t size() {
            return _size;
        }


        void set_bool(as_boolean* b) {
            _stack_buf[0] = static_cast<uint8_t>(b->value);
            _buf = _stack_buf;
            _size = 1;
            _free = false;
        }

        void set_integer(as_integer* i);

        void set_double(as_double* d);

        void set_string(as_string* str) {
            _buf = reinterpret_cast<uint8_t*>(str->value);
            _size = static_cast<uint32_t>(strlen(str->value));
            _free = false;
        }

        void set_bytes(as_bytes* bytes) {
            _buf = bytes->value;
            _size = bytes->size;
            _free = false;
        }

        void set_with_serializer(as_val* val);


    private:

        uint8_t  _stack_buf[sizeof(uint64_t)];
        uint8_t* _buf;
        uint32_t _size;
        bool _free;

    };


}

