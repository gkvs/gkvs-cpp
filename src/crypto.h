#pragma once

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace gkvs {

    std::string hash_ripemd160(const char* pstr);

    std::string hash_ripemd160(const uint8_t *data, uint32_t size);

}


