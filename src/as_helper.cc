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


#include <aerospike/aerospike_key.h>
#include <citrusleaf/cf_byte_order.h>

#include <aerospike/as_serializer.h>
#include <aerospike/as_msgpack.h>


#include "as_driver.h"

void gkvs::as_value_binarer::set_integer(as_integer* i) {

    _size = sizeof(uint64_t);
    _buf = _stack_buf;
    _free = false;

    *(uint64_t*) _buf = cf_swap_to_le64(i->value);
}

void gkvs::as_value_binarer::set_double(as_double* d) {

    _size = sizeof(double);
    _buf = _stack_buf;
    _free = false;

    *(double*) _buf = cf_swap_to_little_float64(d->value);

}

void gkvs::as_value_binarer::set_with_serializer(as_val* val) {

    as_buffer buffer;
    as_buffer_init(&buffer);
    as_serializer ser;
    as_msgpack_init(&ser);
    as_serializer_serialize(&ser, val, &buffer);
    as_serializer_destroy(&ser);

    _size = buffer.size;
    _buf = new uint8_t[_size];
    _free = true;
    memcpy(_buf, buffer.data, _size);

    as_buffer_destroy(&buffer);
}


void gkvs::as_value_binarer::set(as_key_value* value) {

    reset();

    if (value != nullptr) {
        as_val_t type = as_val_type(value);

        switch(type) {

            case AS_INTEGER:
                set_integer(&value->integer);
                break;

            case AS_STRING:
                set_string(&value->string);
                break;

            case AS_BYTES:
                set_bytes(&value->bytes);
                break;

            default:
                set_with_serializer(&value->bytes._);
                break;
        }

    }


}


void gkvs::as_value_binarer::set(as_bin_value* value) {

    reset();

    if (value != nullptr) {
        as_val_t type = as_val_type(value);

        switch(type) {

            case AS_NIL:
                break;

            case AS_INTEGER:
                set_integer(&value->integer);
                break;

            case AS_DOUBLE:
                set_double(&value->dbl);
                break;

            case AS_STRING:
                set_string(&value->string);
                break;

            case AS_BYTES:
                set_bytes(&value->bytes);
                break;

            case AS_LIST:
                set_with_serializer(&value->list._);
                break;

            case AS_MAP:
                set_with_serializer(&value->map._);
                break;

            default:
                set_with_serializer(&value->bytes._);
                break;

        }


    }

}

void gkvs::as_value_binarer::set(as_val* value) {

    reset();

    if (value != nullptr) {
        as_val_t type = as_val_type(value);

        switch(type) {

            case AS_UNDEF:
                break;

            case AS_NIL:
                break;

            case AS_BOOLEAN:
                set_bool((as_boolean*) value);
                break;

            case AS_INTEGER:
                set_integer((as_integer*) value);
                break;

            case AS_STRING:
                set_string((as_string*) value);
                break;

            case AS_LIST:
            case AS_MAP:
            case AS_REC:
            case AS_PAIR:
            case AS_GEOJSON:
                set_with_serializer(value);
                break;

            case AS_BYTES:
                set_bytes((as_bytes*) value);
                break;

            case AS_DOUBLE:
                set_double((as_double*) value);
                break;

            default:
                set_with_serializer(value);
                break;

        }

    }

}

size_t gkvs::as_key_hash::operator()(const as_key *key) const {

    as_digest *digest = as_key_digest((as_key *) key);
    if (digest) {
        const as_digest_value &b = key->digest.value;
        int size_len = sizeof(size_t);
        size_t hash = 0;
        for (int i = 1; i <= size_len; ++i) {
            hash <<= 8;
            hash |= b[size_len - i];
        }
        return hash;
    }

    return 0;
}

bool gkvs::as_key_equal::operator()(const as_key *lhs, const as_key *rhs) const {

    if (lhs == rhs) {
        return true;
    }


    as_digest *ldigest = as_key_digest((as_key *) lhs);
    as_digest *rdigest = as_key_digest((as_key *) rhs);

    if (!ldigest || !rdigest) {
        return false;
    }

    const as_digest_value &lval = ldigest->value;
    const as_digest_value &rval = rdigest->value;

    for (int i = 0; i < AS_DIGEST_VALUE_SIZE; ++i) {
        uint8_t lch = lval[i];
        uint8_t rch = rval[i];

        if (lch != rch) {
            return false;
        }
    }


    for (int i = 0; i < AS_NAMESPACE_MAX_SIZE; ++i) {
        char lch = lhs->ns[i];
        char rch = rhs->ns[i];

        if (lch != rch) {
            return false;
        }

        if (!lch) {
            break;
        }
    }

    for (int i = 0; i < AS_SET_MAX_SIZE; ++i) {
        char lch = lhs->set[i];
        char rch = rhs->set[i];

        if (lch != rch) {
            return false;
        }

        if (!lch) {
            break;
        }

    }

    return true;

}
