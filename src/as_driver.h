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

#pragma once

#include <aerospike/aerospike_key.h>
#include <aerospike/as_boolean.h>


#include <iostream>

namespace gkvs {

    struct as_key_hash {

        size_t operator()(const as_key *key) const;

    };

    struct as_key_equal {

        bool operator()(const as_key *lhs, const as_key *rhs) const;

    };

    class as_value_binarer final {

    public:

        as_value_binarer() {
            _buf = nullptr;
            _free = false;
        }

        ~as_value_binarer() {
            if (_free && _buf != nullptr) {
                delete _buf;
            }
        }

        void reset() {
            if (_free && _buf != nullptr) {
                delete _buf;
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

