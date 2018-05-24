/*
 *
 * Copyright 2018 gKVS authors.
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

#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <cassert>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "helper.h"
#include "gkvs.grpc.pb.h"

#include <aerospike/as_arraylist.h>
#include <aerospike/as_hashmap.h>

#include <glog/logging.h>
#include "gflags/gflags.h"

#include "as_driver.h"


using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

namespace gkvs {

    std::string to_hex(const uint8_t *data, int len) {
        std::stringstream ss;
        ss << std::hex;
        for (int i = 0; i < len; ++i) {
            ss << (int) data[i];
        }
        return ss.str();
    }

    bool assert_eq(bool eq, const char* str) {
        if (!eq) {
            std::cout << str << std::endl;
            return false;
        }
        return true;
    }

    bool as_key_test() {

        bool passed = true;

        as_key key;
        as_key_init_str(&key, "test", "CACHE", "val1");

        gkvs::as_key_hash hasher;
        size_t hash = hasher(&key);

        int size_len = sizeof(size_t);
        for (int i = 0; i != size_len; ++i) {
            uint8_t ch = static_cast<uint8_t>(hash & 0xFF);
            uint8_t h = key.digest.value[i];

            if (ch != h) {
                std::cout << "as_key_test fail" << std::endl;
                std::cout << "hashValue = " << to_hex(key.digest.value, AS_DIGEST_VALUE_SIZE) << std::endl;
                std::cout << std::hex << hash << std::dec << std::endl;
                passed = false;
                break;
            }

            hash >>= 8;
        }

        // test eq

        gkvs::as_key_equal eq;

        if (!eq(&key, &key)) {
            std::cout << "as_key_test fail, eq same ref" << std::endl;
            passed = false;
        }

        as_key key_same;
        as_key_init_str(&key_same, "test", "CACHE", "val1");

        if (!eq(&key, &key_same)) {
            std::cout << "as_key_test fail, eq same value" << std::endl;
            passed = false;
        }

        as_key key_diff;
        as_key_init_str(&key_diff, "test", "CACHE", "val2");

        if (eq(&key, &key_diff)) {
            std::cout << "as_key_test fail, eq diff value" << std::endl;
            passed = false;
        }

        // test map

        std::unordered_map<const as_key *, int, gkvs::as_key_hash, gkvs::as_key_equal> key_map;

        key_map[&key] = 555;
        key_map[&key_same] = 777;
        key_map[&key_diff] = 999;

        if (key_map.size() != 2) {
            std::cout << "as_key_test fail, wrong size of key_map" << std::endl;
            passed = false;
        }

        if (key_map[&key] != 777) {
            std::cout << "as_key_test fail, wrong value for key" << std::endl;
            passed = false;
        }

        if (key_map[&key_diff] != 999) {
            std::cout << "as_key_test fail, wrong value of key_diff" << std::endl;
            passed = false;
        }

        return passed;
    }

    bool as_interger_tests() {

        bool passed = true;

        as_integer n;
        as_integer_init(&n, 1);

        as_value_binarer binarer;
        binarer.set((as_val*) &n);

        passed &= assert_eq(binarer.size() == sizeof(uint64_t), "as_value_binarer as_integer fail 1");
        passed &= assert_eq(binarer.data()[0] == 1, "as_value_binarer as_integer fail 2");
        for (int i = 1; i < sizeof(uint64_t); ++i) {
            passed &= assert_eq(binarer.data()[i] == 0, "as_value_binarer as_integer fail 3");
        }

        as_integer_init(&n, 0x3435363738393A3B);
        binarer.set((as_val*) &n);
        passed &= assert_eq(binarer.size() == sizeof(uint64_t), "as_value_binarer as_integer fail 4");
        for (int i = 0; i < sizeof(uint64_t); ++i) {
            passed &= assert_eq(binarer.data()[i] == 0x3B - i, "as_value_binarer as_integer fail 5");
        }

        return passed;

    }

    bool as_double_tests() {

        bool passed = true;

        as_double d;
        as_double_init(&d, 1.0);

        as_value_binarer binarer;
        binarer.set((as_val*) &d);

        passed &= assert_eq(binarer.size() == sizeof(uint64_t), "as_value_binarer as_double fail 1");

        std::string str = to_hex(binarer.data(), binarer.size());
        passed &= assert_eq(strcmp("000000f03f", str.c_str()) == 0, "as_value_binarer as_double fail 2");

        return passed;

    }

    bool as_string_tests() {

        bool passed = true;

        as_string* s = as_string_new((char*) "alex", false);

        as_value_binarer binarer;
        binarer.set((as_val*) s);

        passed &= assert_eq(binarer.size() == 4, "as_value_binarer as_string fail 1");
        passed &= assert_eq(strncmp("alex", (const char*) binarer.data(), 4) == 0, "as_value_binarer as_string fail 2");

        as_string_destroy(s);

        return passed;

    }

    bool as_bytes_tests() {

        bool passed = true;

        as_bytes* s = as_bytes_new_wrap((uint8_t *) "alex", 4, false);

        as_value_binarer binarer;
        binarer.set((as_val*) s);

        passed &= assert_eq(binarer.size() == 4, "as_value_binarer as_bytes fail 1");
        passed &= assert_eq(strncmp("alex", (const char*) binarer.data(), 4) == 0, "as_value_binarer as_bytes fail 2");

        as_bytes_destroy(s);

        return passed;

    }

    bool as_list_tests() {

        bool passed = true;

        as_arraylist* al = as_arraylist_new(10, 10);
        as_list* l = (as_list*) al;

        as_list_append_str(l, "alex");
        as_list_append_str(l, "shvid");

        as_value_binarer binarer;
        binarer.set((as_val*) l);

        std::string expected = "92a53616c6578a637368766964"; // msgpack
        std::string actual = to_hex(binarer.data(), binarer.size());

        passed &= assert_eq(expected == actual, "as_value_binarer as_list fail 1");

        as_arraylist_destroy(al);

        return passed;

    }

    bool as_map_tests() {

        bool passed = true;

        as_hashmap *hm = as_hashmap_new(10);
        as_map *m = (as_map*) hm;

        as_string* key = as_string_new((char*) "alex", false);
        as_string* value = as_string_new((char*) "shvid", false);

        as_map_set(m, (const as_val*)key, (const as_val*)value);

        as_value_binarer binarer;
        binarer.set((as_val*) m);

        std::string expected = "81a53616c6578a637368766964"; // msgpack
        std::string actual = to_hex(binarer.data(), binarer.size());

        passed &= assert_eq(expected == actual, "as_value_binarer as_map fail 1");

        as_hashmap_destroy(hm);
        as_string_destroy(key);
        as_string_destroy(value);

        return passed;

    }

    bool as_val_binarer_tests() {

        bool passed = true;

        passed &= as_interger_tests();
        passed &= as_double_tests();
        passed &= as_string_tests();
        passed &= as_bytes_tests();
        passed &= as_list_tests();
        passed &= as_map_tests();

        return passed;

    }

    bool as_run_tests() {

        bool passed = true;

        passed &= as_key_test();
        passed &= as_val_binarer_tests();

        return passed;

    }

}