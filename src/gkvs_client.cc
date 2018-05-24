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


DEFINE_bool(run_tests, false, "Run functional tests");


void run_tests() {

    bool passed = true;

    if (passed) {
        std::cout << "SUCCESS" << std::endl;
    }
    else {
        std::cout << "FAILURE" << std::endl;
    }

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
