#pragma once

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace gkvs {

    class Ripend160Hash final {

    public:

        Ripend160Hash() {
            memset(_hash, 0, _size);
        }

         void apply(const char* pstr);

         void apply(const uint8_t *data, uint32_t size);

         inline size_t size() {
             return _size;
         }

         inline const uint8_t* data() {
            return _hash;
         }

    private:

        const static size_t _size = 20;
        uint8_t _hash[_size];

    };


}


