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

extern "C" {

//#include <lua.h>
//#include <lauxlib.h>
//#include <lualib.h>

#include <luajit-2.0/lua.h>
#include <luajit-2.0/lauxlib.h>

}

#include <msgpack.h>


namespace gkvs {

    class lua_ser {

    public:

        explicit lua_ser() : sbuf_free_(false), sbuf_pos_(0), mempool_free_(false) {
        }

        ~lua_ser() {
            if (sbuf_free_) {
                msgpack_sbuffer_destroy(&sbuf_);
            }
            if (mempool_free_) {
                msgpack_zone_destroy(&mempool_);
            }
        }

        void unpack_obj(const msgpack_object& obj, lua_State* L);

        void pack_obj(lua_State* L, int index);

        const char* data() {
            if (!sbuf_free_) {
                return nullptr;
            }
            return &sbuf_.data[sbuf_pos_];
        }

        size_t size() {
            if (!sbuf_free_) {
                return 0;
            }
            return sbuf_.size - sbuf_pos_;
        }

    protected:

        void unpack_array(const msgpack_object_array& array, lua_State* L);

        void unpack_map(const msgpack_object_map& map, lua_State* L);

        void pack_table(lua_State* L, int index);

        void pack_array(lua_State* L, int index);

        void pack_map(lua_State* L, int index);

    private:

        msgpack_packer pk_;

        msgpack_sbuffer sbuf_;
        bool sbuf_free_;
        size_t sbuf_pos_;

        msgpack_zone mempool_;
        bool mempool_free_;

    };

}