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


#include <aerospike/aerospike_key.h>
#include <citrusleaf/cf_byte_order.h>

#include <aerospike/as_serializer.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_map_iterator.h>
#include <aerospike/as_hashmap_iterator.h>

#include <unordered_map>
#include <sstream>
#include <algorithm>

#include <stdlib.h>

#include <msgpack.h>
#include <msgpack.hpp>
#include <msgpack/object.h>

#include "as_driver.h"

/**
void gkvs::as_record_ser::pack_val_ser(as_val* val) {

    as_buffer buffer;
    as_buffer_init(&buffer);
    as_serializer ser;
    as_msgpack_init(&ser);
    as_serializer_serialize(&ser, val, &buffer);
    as_serializer_destroy(&ser);

    msgpack_pack_v4raw(&pk_, buffer.size);
    msgpack_pack_v4raw_body(&pk_, buffer.data, buffer.size);

    as_buffer_destroy(&buffer);
}
 **/


void gkvs::as_record_ser::pack_list(as_list* list) {

    uint32_t size = as_list_size(list);
    msgpack_pack_array(&pk_, size);

    as_arraylist_iterator it;
    as_arraylist_iterator_init(&it, (as_arraylist*) list);

    while (as_arraylist_iterator_has_next(&it)) {

        as_val *val = (as_val*) as_arraylist_iterator_next(&it);
        pack_val(val);

    }

}

void gkvs::as_record_ser::pack_map(as_map* map) {

    uint32_t size = as_map_size(map);
    msgpack_pack_map(&pk_, size);

    as_hashmap_iterator it;
    as_hashmap_iterator_init(&it, (as_hashmap*) map);

    while (as_hashmap_iterator_has_next(&it)) {

        as_val *el = (as_val*) as_hashmap_iterator_next(&it);

        if (el != nullptr && el->type == AS_PAIR) {

            as_pair *kv = (as_pair *) el;

            pack_val(as_pair_1(kv));
            pack_val(as_pair_2(kv));

        }

    }

}

void gkvs::as_record_ser::pack_val(as_val* value) {

    if (value == nullptr) {
        msgpack_pack_nil(&pk_);
        return;
    }

    as_val_t type = as_val_type(value);

    switch(type) {

        case AS_NIL:
            msgpack_pack_nil(&pk_);
            break;

        case AS_BOOLEAN:
            if (((as_boolean*) value)->value) {
                msgpack_pack_true(&pk_);
            }
            else {
                msgpack_pack_false(&pk_);
            }
            break;

        case AS_INTEGER:
            msgpack_pack_int64(&pk_, ((as_integer*) value)->value);
            break;

        case AS_DOUBLE:
            msgpack_pack_double(&pk_, ((as_double*) value)->value);
            break;

        case AS_STRING: {
            as_string* str = (as_string*) value;
            size_t len = strlen(str->value);
            msgpack_pack_str(&pk_, len);
            msgpack_pack_str_body(&pk_, str->value, len);
            break;
        }

        case AS_BYTES: {
            as_bytes* bytes = (as_bytes*) value;
            msgpack_pack_v4raw(&pk_, bytes->size);
            msgpack_pack_v4raw_body(&pk_, bytes->value, bytes->size);
            break;
        }

        case AS_LIST:
            pack_list((as_list*) value);
            break;

        case AS_MAP:
            pack_map((as_map*) value);
            break;

        case AS_GEOJSON: {
            as_geojson* geo = (as_geojson*)  value;
            size_t len = strlen(geo->value);
            msgpack_pack_str(&pk_, len);
            msgpack_pack_str_body(&pk_, geo->value, len);
            break;
        }

        default: {
            char* str = as_val_tostring(value);
            size_t len = strlen(str);
            msgpack_pack_str(&pk_, len);
            msgpack_pack_str_body(&pk_, str, len);
            cf_free(str);
            break;
        }
    }

}


void gkvs::as_record_ser::pack_bin_value(as_bin_value *value) {

    as_val_t type = as_val_type(value);

    switch(type) {

        case AS_NIL:
            msgpack_pack_nil(&pk_);
            break;

        case AS_INTEGER:
            msgpack_pack_int64(&pk_, value->integer.value);
            break;

        case AS_DOUBLE:
            msgpack_pack_double(&pk_, value->dbl.value);
            break;

        case AS_STRING: {
            size_t len = strlen(value->string.value);
            msgpack_pack_str(&pk_, len);
            msgpack_pack_str_body(&pk_, value->string.value, len);
            break;
        }

        case AS_BYTES:
            msgpack_pack_v4raw(&pk_, value->bytes.size);
            msgpack_pack_v4raw_body(&pk_, value->bytes.value, value->bytes.size);
            break;

        case AS_LIST:
            pack_list(&value->list);
            break;

        case AS_MAP:
            pack_map(&value->map);
            break;

        default: {
            char *str = as_val_tostring(value);
            size_t len = strlen(str);
            msgpack_pack_str(&pk_, len);
            msgpack_pack_str_body(&pk_, str, len);
            cf_free(str);
            break;
        }
    }

}


bool gkvs::as_record_ser::pack_record(as_record *rec, bool single_bin) {

    if (rec_free_) {
        as_record_destroy(rec_);
    }
    rec_ = rec;
    rec_free_ = false;

    if (!sbuf_free_) {
        msgpack_sbuffer_init(&sbuf_);
        sbuf_free_ = true;
        msgpack_packer_init(&pk_, &sbuf_, msgpack_sbuffer_write);
    }

    sbuf_pos_ = sbuf_.size;

    uint16_t size = as_record_numbins(rec_);

    if (single_bin) {

        if (size >= 1) {
            as_bin_value *value = as_bin_get_value(rec_->bins.entries);
            pack_bin_value(value);
        }

        return true;

    }

    if (size == 1) {

        char* str = as_bin_get_name(rec_->bins.entries);
        int first_len = strlen(str);

        if (first_len == 0) {

            as_bin_value *value = as_bin_get_value(rec_->bins.entries);
            pack_bin_value(value);
            return true;
        }
    }

    msgpack_pack_map(&pk_, size);

    as_record_iterator it;
    as_record_iterator_init(&it, rec_);

    while (as_record_iterator_has_next(&it)) {

        const as_bin* bin = as_record_iterator_next(&it);

        char* key = as_bin_get_name(bin);
        size_t key_len = strlen(key);

        msgpack_pack_str(&pk_, key_len);
        msgpack_pack_str_body(&pk_, key, key_len);

        as_bin_value* value = as_bin_get_value(bin);
        pack_bin_value(value);

    }

    return true;
}

as_list* gkvs::as_record_ser::unpack_list(const msgpack_object &val_obj) {

    const msgpack_object_array &array = val_obj.via.array;

    as_list* list = (as_list*) as_arraylist_new(array.size, array.size);

    for (uint32_t i = 0; i < array.size; ++i) {

        msgpack_object& val_obj = array.ptr[i];

        as_list_append(list, unpack_val(val_obj));

    }

    return list;
}

as_map* gkvs::as_record_ser::unpack_map(const msgpack_object &val_obj) {

    const msgpack_object_map &map = val_obj.via.map;

    as_map* hashmap = (as_map*) as_hashmap_new(map.size);

    for (uint32_t i = 0; i < map.size; ++i) {

        msgpack_object& key_obj = map.ptr[i].key;
        msgpack_object& val_obj = map.ptr[i].val;

        as_map_set(hashmap, unpack_val(key_obj), unpack_val(val_obj));

    }

    return hashmap;
}

as_val* gkvs::as_record_ser::unpack_val(const msgpack_object &val_obj) {

    switch (val_obj.type) {

        case MSGPACK_OBJECT_NIL:
            return (as_val*) &as_nil;

        case MSGPACK_OBJECT_BOOLEAN:
            return as_boolean_toval(as_boolean_new(val_obj.via.boolean));

        case MSGPACK_OBJECT_POSITIVE_INTEGER:
            return as_integer_toval(as_integer_new(val_obj.via.u64));

        case MSGPACK_OBJECT_NEGATIVE_INTEGER:
            return as_integer_toval(as_integer_new(val_obj.via.i64));

        case MSGPACK_OBJECT_FLOAT32:
        case MSGPACK_OBJECT_FLOAT64:
            return as_double_toval(as_double_new(val_obj.via.f64));

        case MSGPACK_OBJECT_STR: {
            /**
             *
             * DO NOT ALLOCATE IN HEAP
             *
            int len = val_obj.via.str.size;
            char* buf = (char*) cf_malloc(len+1);
            memcpy(buf, val_obj.via.str.ptr, len);
            buf[len] = 0;
            return as_string_toval(as_string_new(buf, true));
             **/
            return as_string_toval(as_string_new_wlen((char*)val_obj.via.str.ptr, val_obj.via.str.size, false));
        }

        case MSGPACK_OBJECT_BIN: {
            /**
             *
             * DO NOT ALLOCATE IN HEAP
             *
            int len = val_obj.via.bin.size;
            uint8_t* buf = (uint8_t*) cf_malloc(len);
            memcpy(buf, val_obj.via.bin.ptr, len);
            return as_bytes_toval(as_bytes_new_wrap(buf, len, true));
             **/
            return as_bytes_toval(as_bytes_new_wrap((uint8_t *)val_obj.via.bin.ptr, val_obj.via.bin.size, false));
        }

        case MSGPACK_OBJECT_ARRAY:
            return as_list_toval(unpack_list(val_obj));

        case MSGPACK_OBJECT_MAP:
            return as_map_toval(unpack_map(val_obj));

        default:
            return (as_val*) &as_nil;

    }

}

void gkvs::as_record_ser::record_set(const char* key, const msgpack_object& val_obj) {


    switch (val_obj.type) {

        case MSGPACK_OBJECT_NIL:
            as_record_set_nil(rec_, key);
            break;

        case MSGPACK_OBJECT_BOOLEAN:
            as_record_set_int64(rec_, key, val_obj.via.boolean ? 1 : 0);
            break;

        case MSGPACK_OBJECT_POSITIVE_INTEGER:
            as_record_set_int64(rec_, key, val_obj.via.u64);
            break;

        case MSGPACK_OBJECT_NEGATIVE_INTEGER:
            as_record_set_int64(rec_, key, val_obj.via.i64);
            break;

        case MSGPACK_OBJECT_FLOAT32:
        case MSGPACK_OBJECT_FLOAT64:
            as_record_set_double(rec_, key, val_obj.via.f64);
            break;

        case MSGPACK_OBJECT_STR: {
            int len = val_obj.via.str.size;
            char* buf = (char*) cf_malloc(len+1);
            memcpy(buf, val_obj.via.str.ptr, len);
            buf[len] = 0;
            as_record_set_strp(rec_, key, buf, true);
            break;
        }

        case MSGPACK_OBJECT_BIN: {
            /**
             *
             * DO NOT ALLOCATE IN HEAP
             *
            int len = val_obj.via.bin.size;
            uint8_t* buf = (uint8_t*) cf_malloc(len);
            memcpy(buf, val_obj.via.bin.ptr, len);
            as_record_set_rawp(rec_, key, buf, len, true);
             **/

            // msgpack val_obj reference on unpacker buffer
            as_record_set_raw(rec_, key, (uint8_t*) val_obj.via.bin.ptr, val_obj.via.bin.size);
            break;
        }

        case MSGPACK_OBJECT_ARRAY:
            as_record_set_list(rec_, key, unpack_list(val_obj));
            break;

        case MSGPACK_OBJECT_MAP:
            as_record_set_map(rec_, key, unpack_map(val_obj));
            break;

        default: {

            if (!sbuf_free_) {
                msgpack_sbuffer_init(&sbuf_);
                sbuf_free_ = true;
                msgpack_packer_init(&pk_, &sbuf_, msgpack_sbuffer_write);
            }

            sbuf_pos_ = sbuf_.size;
            msgpack_pack_object(&pk_, val_obj);

            as_record_set_raw(rec_, key, (uint8_t *) this->data(), this->size());
            break;
        }


    }

}

as_record* gkvs::as_record_ser::alloc_rec(size_t size) {

    if (rec_free_) {
        as_record_destroy(rec_);
    }
    rec_ = as_record_new(size);
    rec_free_ = true;

    return rec_;
}

void gkvs::as_record_ser::stringify(msgpack_object& obj, char* buf, uint32_t size) {

    switch(obj.type) {

        case MSGPACK_OBJECT_STR: {
            size_t len = std::min(size - 1, obj.via.str.size);
            memcpy(buf, obj.via.str.ptr, len);
            buf[len] = 0;
            break;
        }

        case MSGPACK_OBJECT_POSITIVE_INTEGER:
            snprintf(buf, size, "%lu", obj.via.u64);
            break;

        case MSGPACK_OBJECT_NEGATIVE_INTEGER:
            snprintf(buf, size, "%ld", obj.via.i64);
            break;

        default:
            msgpack_object_print_buffer(buf, size, obj);
            break;

    }

}


as_record* gkvs::as_record_ser::unpack_record(const char* data, size_t size, bool single_bin) {

    if (!mempool_free_) {
        msgpack_zone_init(&mempool_, 2048);
        mempool_free_ = true;
    }

    msgpack_object deserialized;
    msgpack_unpack(data, size, NULL, &mempool_, &deserialized);

    //msgpack_object_print(stdout, deserialized);

    if (single_bin || deserialized.type != MSGPACK_OBJECT_MAP) {
        // use default column "" and single value

        alloc_rec(1);
        record_set((char*)"", deserialized);

        return rec_;
    }

    msgpack_object_map map = deserialized.via.map;

    alloc_rec(map.size);

    for (uint32_t i = 0; i < map.size; ++i) {

        msgpack_object& key_obj = map.ptr[i].key;
        msgpack_object& val_obj = map.ptr[i].val;

        char bin[AS_BIN_NAME_MAX_SIZE];
        stringify(key_obj, bin, sizeof(bin));

        record_set(bin, val_obj);

    }

    return rec_;


}


void gkvs::as_value_ser::set_integer(as_integer* i) {

    _size = sizeof(uint64_t);
    _buf = _stack_buf;
    _free = false;

    *(uint64_t*) _buf = cf_swap_to_le64(i->value);
}

void gkvs::as_value_ser::set_double(as_double* d) {

    _size = sizeof(double);
    _buf = _stack_buf;
    _free = false;

    *(double*) _buf = cf_swap_to_little_float64(d->value);

}

void gkvs::as_value_ser::set_with_serializer(as_val* val) {

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


void gkvs::as_value_ser::set(as_key_value* value) {

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


void gkvs::as_value_ser::set(as_bin_value* value) {

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

void gkvs::as_value_ser::set(as_val* value) {

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






