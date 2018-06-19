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

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <cerrno>
#include <unistd.h>

#include "gkvs.grpc.pb.h"
#include <gflags/gflags.h>



DEFINE_string(gkvs_keys, "", "gkvs-keys path");
DEFINE_string(gkvs_hostname, "", "gkvs-keys hostname");

namespace gkvs {

    std::string get_keys() {
        if (!FLAGS_gkvs_keys.empty()) {
            return FLAGS_gkvs_keys;
        }

        char* env_gkvs_keys = getenv("GKVS_KEYS");
        if (env_gkvs_keys != nullptr) {
            return std::string(env_gkvs_keys);
        }

        return ".";

    }

    std::string get_hostname() {

        if (!FLAGS_gkvs_hostname.empty()) {
            return FLAGS_gkvs_hostname;
        }

        char* env_gkvs_hostname = getenv("GKVS_HOSTNAME");
        if (env_gkvs_hostname != nullptr) {
            return std::string(env_gkvs_hostname);
        }

        char* env_hostname = getenv("HOSTNAME");
        if (env_hostname != nullptr) {
            return std::string(env_hostname);
        }

        char tmp[512];
        if (gethostname(tmp, 512) == 0) { // success = 0, failure = -1
            return std::string(tmp);
        }

        return "localhost";
    }

    std::string get_file_content(const std::string& filename) {

        std::ifstream in(filename, std::ios::in | std::ios::binary);
        if (in)
        {
            return(std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>()));
        }

        std::ostringstream msg;
        msg << "file not found: " << filename << ", errno=" << errno;

        throw std::runtime_error(msg.str());
    }


}
