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

#include <string>
#include <vector>

namespace gkvs {

    bool add_cluster(const std::string& cluster, const std::string& driver, const std::string& conf, std::string& error);

    bool add_table(const std::string& table, const std::string& cluster, const std::string& conf, std::string& error);

    bool add_view(const std::string& view, const std::string& cluster, const std::string& table, std::string& error);

    // clusterName:string, driver:string, conf:table
    static int lua_add_cluster(lua_State *L);

    // tableName:string, clusterName:string, conf:table
    static int lua_add_table(lua_State *L);

    // viewName:string, clusterName:string, tableName:string
    static int lua_add_view(lua_State *L);

    class lua_script {

        public:

        explicit lua_script();

        ~lua_script() {
            lua_close(L);
        }

        bool loadfile(const std::string& filename, std::string& error);

        bool loadstring(const std::string& content, std::string& error);

        template<typename T>
        inline T get(const std::string& variableName) const {
            int level = 0;
            T result;
            if(lua_gettostack(variableName, level)) {
                result = lua_get<T>(variableName);
            } else {
                result = lua_getdefault<T>(variableName);
            }
            lua_pop(L, level + 1); // pop all existing elements from stack
            return result;
        }

        template<typename T>
        inline T get(const std::string& variableName, T defaultValue) const {
            int level = 0;
            T result;
            if(lua_gettostack(variableName, level)) {
                result = lua_get<T>(variableName);
            } else {
                result = defaultValue;
            }
            lua_pop(L, level + 1); // pop all existing elements from stack
            return result;
        }

    protected:

        void print_error(const std::string& variableName, const std::string& reason) const;

        bool lua_gettostack(const std::string& variableName, int& level) const;

        // Generic get
        template<typename T>
        T lua_get(const std::string& variableName) const;

        // Generic default get
        template<typename T>
        T lua_getdefault(const std::string& variableName) const;

    private:
        lua_State* L;


    };

    template<>
    inline bool lua_script::lua_getdefault(const std::string& variableName) const {
        return false;
    }

    template<>
    inline double lua_script::lua_getdefault(const std::string& variableName) const {
        return 0.0;
    }

    template<>
    inline int lua_script::lua_getdefault(const std::string& variableName) const {
        return 0;
    }

    template<>
    inline std::string lua_script::lua_getdefault(const std::string &variableName) const {
        return "";
    }

    template<>
    inline bool lua_script::lua_get(const std::string &variableName) const {
        return (bool)lua_toboolean(L, -1);
    }

    template <>
    inline double lua_script::lua_get(const std::string& variableName) const {
        if(!lua_isnumber(L, -1)) {
            print_error(variableName, "Not a number");
        }
        return (double)lua_tonumber(L, -1);
    }

    template <>
    inline int lua_script::lua_get(const std::string& variableName) const {
        if(!lua_isnumber(L, -1)) {
            print_error(variableName, "Not a number");
        }
        return lua_tointeger(L, -1);
    }

    template <>
    inline std::string lua_script::lua_get(const std::string& variableName) const {
        std::string s = "";
        if(lua_isstring(L, -1)) {
            s = std::string(lua_tostring(L, -1));
        } else {
            print_error(variableName, "Not a string");
        }
        return s;
    }

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

        void print();

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