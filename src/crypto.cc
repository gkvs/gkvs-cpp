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

#include <openssl/ripemd.h>

#include "crypto.h"


std::string gkvs::hash_ripemd160(const char* pstr) {

    unsigned char digest[RIPEMD160_DIGEST_LENGTH];
    RIPEMD160( (const uint8_t*) pstr, strlen(pstr) + 1, digest);

    return std::string((char*) digest, RIPEMD160_DIGEST_LENGTH);

}

std::string gkvs::hash_ripemd160(const uint8_t* data, uint32_t size) {

    unsigned char digest[RIPEMD160_DIGEST_LENGTH];
    RIPEMD160(data, size, digest);

    return std::string((char*) digest, RIPEMD160_DIGEST_LENGTH);
}


