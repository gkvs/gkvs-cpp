#pragma once

#include <aerospike/aerospike_key.h>


namespace gkvs {

    struct as_key_hash {

        size_t operator()(const as_key *key) const {

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

    };

    struct as_key_equal {

        bool operator()(const as_key *lhs, const as_key *rhs) const {

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

    };


}

#define AS_MAX_LOG_STR 1024
static bool glog_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...);

static gkvs::Status_Code parse_aerospike_status(as_status status);