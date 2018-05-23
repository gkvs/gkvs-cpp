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

#include <glog/logging.h>
#include "gflags/gflags.h"

#include "as_driver.h"


using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;


DEFINE_bool(run_tests, false, "Run functional tests client-server");

std::string to_hex(uint8_t *data, int len)
{
    std::stringstream ss;
    ss<<std::hex;
    for(int i = 0;i<len;++i) {
        ss << (int) data[i];
    }
    return ss.str();
}

void as_key_test() {

    as_key key;
    as_key_init_str(&key, "test", "CACHE", "val1");

    gkvs::as_key_hash hasher;
    size_t hash = hasher(&key);

    int size_len = sizeof(size_t);
    for (int i = 0; i != size_len; ++i) {
        uint8_t ch = hash & 0xFF;
        uint8_t h = key.digest.value[i];

        if (ch != h) {
            std::cout << "as_key_test fail" << std::endl;
            std::cout << "hashValue = " << to_hex(key.digest.value, AS_DIGEST_VALUE_SIZE) << std::endl;
            std::cout << std::hex << hash << std::dec << std::endl;
            break;
        }

        hash >>= 8;
    }

    // test eq

    gkvs::as_key_equal eq;

    if (!eq(&key, &key)) {
        std::cout << "as_key_test fail, eq same ref" << std::endl;
    }

    as_key key_same;
    as_key_init_str(&key_same, "test", "CACHE", "val1");

    if (!eq(&key, &key_same)) {
        std::cout << "as_key_test fail, eq same value" << std::endl;
    }

    as_key key_diff;
    as_key_init_str(&key_diff, "test", "CACHE", "val2");

    if (eq(&key, &key_diff)) {
        std::cout << "as_key_test fail, eq diff value" << std::endl;
    }

    // test map

    std::unordered_map<const as_key*, int, gkvs::as_key_hash, gkvs::as_key_equal> key_map;

    key_map[&key] = 555;
    key_map[&key_same] = 777;
    key_map[&key_diff] = 999;

    if (key_map.size() != 2) {
        std::cout << "as_key_test fail, wrong size of key_map" << std::endl;
    }

    if (key_map[&key] != 777) {
        std::cout << "as_key_test fail, wrong value for key" << std::endl;
    }

    if (key_map[&key_diff] != 999) {
        std::cout << "as_key_test fail, wrong value of key_diff" << std::endl;
    }

}



void run_tests() {

    as_key_test();

    std::cout << "All tests done!" << std::endl;

}


int main(int argc, char** argv) {

    google::InitGoogleLogging(argv[0]);

    gflags::SetUsageMessage("gKVS Server)");
    gflags::SetVersionString("0.1");

    gflags::ParseCommandLineFlags(&argc, &argv,
            /*remove_flags=*/true);


    std::cout << "gKVS Client" << std::endl;


    if (FLAGS_run_tests) {
        run_tests();
    }


    google::ShutdownGoogleLogging();
    gflags::ShutDownCommandLineFlags();

    return 0;
}
