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

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace gkvs {

    class MurMur3 final {

    public:

        explicit MurMur3() {
            seed_ = -1;
        }

        explicit MurMur3(uint32_t seed) {
            seed_ = seed;
        }

        uint32_t  hash32(const std::string& str) {
            return hash32(str.c_str(), str.size());
        }

        uint32_t hash32(const void * key, int len);

        uint64_t* hash128(const std::string& str) {
            return hash128(str.c_str(), str.size());
        }

        uint64_t* hash128(const void * key, int len);

    private:

        uint32_t seed_;
        uint64_t hash_[2]= {0};

    };


    class Ripend160Hash final {

    public:

        Ripend160Hash() {
            memset(_hash, 0, _size);
        }

         void apply(const char* pstr);

         void apply(const char* data, uint32_t size);

         inline size_t size() {
             return _size;
         }

         inline const char* data() {
            return (const char*) _hash;
         }

    private:

        const static size_t _size = 20;
        uint8_t _hash[_size];

    };


}


