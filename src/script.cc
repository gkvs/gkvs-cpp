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

#include "script.h"

#include <iostream>
#include <string>

#include <glog/logging.h>

#define BINARY_PREFIX "binary!"
#define BINARY_PREFIX_LEN 7

#if LUA_VERSION_NUM > 501
    #define lua_objlen lua_rawlen
#endif

static void lua_stackDump (lua_State *L) {
    int i=lua_gettop(L);
    printf(" ----------------  Stack Dump ----------------\n" );
    while(  i   ) {
        int t = lua_type(L, i);
        switch (t) {
            case LUA_TSTRING:
                printf("%d:`%s'\n", i, lua_tostring(L, i));
                break;
            case LUA_TBOOLEAN:
                printf("%d: %s\n",i,lua_toboolean(L, i) ? "true" : "false");
                break;
            case LUA_TNUMBER:
                printf("%d: %g\n",  i, lua_tonumber(L, i));
                break;
            default: printf("%d: %s\n", i, lua_typename(L, t));
            break;
        }
        i--;
    }
    printf("--------------- Stack Dump Finished ---------------\n" );
}

// clusterName:string, driver:string, conf:table
static int gkvs::lua_add_cluster(lua_State *L) {

    int n = lua_gettop(L);
    if (n < 3) {

        LOG(ERROR) << "lua_add_cluster: wrong number of arguments" << std::endl;

        lua_pushstring(L, "add_cluster: wrong number of arguments");
        lua_error(L);
        return 0;
    }

    size_t len;
    const char * str = luaL_checklstring(L, 1, &len);
    std::string cluster(str, len);

    str = luaL_checklstring(L, 2, &len);
    std::string driver(str, len);

    luaL_checktype(L, 3, LUA_TTABLE);
    gkvs::lua_ser ser;
    ser.pack_obj(L, 3);

    //ser.print();

    std::string conf(ser.data(), ser.size());

    std::string error;
    if (!gkvs::add_cluster(cluster, driver, conf, error)) {

        LOG(ERROR) << "error: " << error << std::endl;

        lua_pushstring(L, "add_cluster: wrong number of arguments");
        lua_error(L);
    }

    return 0;
}

// tableName:string, clusterName:string, conf:table
static int gkvs::lua_add_table(lua_State *L) {

    int n = lua_gettop(L);
    if (n < 3) {

        LOG(ERROR) << "lua_add_table: wrong number of arguments" << std::endl;

        lua_pushstring(L, "wrong number of arguments");
        lua_error(L);
        return 0;
    }

    size_t len;
    const char * str = luaL_checklstring(L, 1, &len);
    std::string table(str, len);

    str = luaL_checklstring(L, 2, &len);
    std::string cluster(str, len);

    luaL_checktype(L, 3, LUA_TTABLE);
    gkvs::lua_ser ser;
    ser.pack_obj(L, 3);

    std::string conf(ser.data(), ser.size());

    std::string error;
    if (!gkvs::add_table(table, cluster, conf, error)) {

        LOG(ERROR) << "error: " << error << std::endl;

        lua_pushstring(L, "add_table: wrong number of arguments");
        lua_error(L);
    }

    return 0;
}

// viewName:string, clusterName:string, tableName:string
static int gkvs::lua_add_view(lua_State *L) {

    int n = lua_gettop(L);
    if (n < 3) {

        LOG(ERROR) << "lua_add_view: wrong number of arguments" << std::endl;

        lua_pushstring(L, "add_view: wrong number of arguments");
        lua_error(L);
        return 0;
    }

    size_t len;
    const char * str = luaL_checklstring(L, 1, &len);
    std::string view(str, len);

    str = luaL_checklstring(L, 2, &len);
    std::string cluster(str, len);

    str = luaL_checklstring(L, 3, &len);
    std::string table(str, len);

    std::string error;
    if (!gkvs::add_view(view, cluster, table, error)) {

        LOG(ERROR) << "error: " << error << std::endl;

        lua_pushstring(L, "add_view: wrong number of arguments");
        lua_error(L);
    }

    return 0;
}


gkvs::lua_script::lua_script() {

    L = luaL_newstate();

    lua_register (L, "add_cluster", &gkvs::lua_add_cluster);
    lua_register (L, "add_table", &gkvs::lua_add_table);
    lua_register (L, "add_view", &gkvs::lua_add_view);

}

bool gkvs::lua_script::loadfile(const std::string& filename) {
    if(luaL_loadfile(L, filename.c_str()) || lua_pcall(L, 0, 0, 0)) {
        LOG(ERROR) << "fail to load LUA script from file: " << filename << std::endl;
        return false;
    }
    return true;
}

bool gkvs::lua_script::loadstring(const std::string& content) {
    if(luaL_loadstring(L, content.c_str()) || lua_pcall(L, 0, 0, 0)) {
        LOG(ERROR) << "fail to exec LUA script: " << content << std::endl;
        return false;
    }
    return true;
}

void gkvs::lua_script::print_error(const std::string& variableName, const std::string& reason) const {
    LOG(ERROR) <<"can't get ["<< variableName << "]: "<< reason <<std::endl;
}

bool gkvs::lua_script::lua_gettostack(const std::string& variableName, int& level) const {
    std::string var = "";
    for(unsigned int i = 0; i < variableName.size(); i++) {
        if(variableName.at(i) == '.') {
            if(level == 0) {
                lua_getfield(L, LUA_GLOBALSINDEX, var.c_str());
            } else {
                lua_getfield(L, -1, var.c_str());
            }

            if(lua_isnil(L, -1)) {
                print_error(variableName, var + " is not defined");
                return false;
            } else {
                var = "";
                level++;
            }
        } else {
            var += variableName.at(i);
        }
    }
    if(level == 0) {
        lua_getfield(L, LUA_GLOBALSINDEX, var.c_str());
    } else {
        lua_getfield(L, -1, var.c_str());
    }
    if(lua_isnil(L, -1)) {
        print_error(variableName, var + " is not defined");
        return false;
    }

    return true;
}


void gkvs::lua_ser::unpack_array(const msgpack_object_array& array, lua_State* L) {

    lua_newtable(L);

    for (uint32_t i = 0; i < array.size; ++i) {
        unpack_obj(array.ptr[i], L);
        lua_rawseti(L, -2, i + 1);
    }

}

void gkvs::lua_ser::unpack_map(const msgpack_object_map& map, lua_State* L) {

    lua_newtable(L);

    for (uint32_t i = 0; i < map.size; ++i) {
        unpack_obj(map.ptr[i].key, L);
        unpack_obj(map.ptr[i].val, L);
        lua_rawset(L, -3);
    }

}

void gkvs::lua_ser::unpack_obj(const msgpack_object& obj, lua_State* L) {

    switch(obj.type) {

        case MSGPACK_OBJECT_NIL:
            lua_pushnil(L);
            break;

        case MSGPACK_OBJECT_BOOLEAN:
            lua_pushboolean(L, obj.via.boolean);
            break;

        case MSGPACK_OBJECT_POSITIVE_INTEGER:
            lua_pushnumber(L, obj.via.u64);
            break;

        case MSGPACK_OBJECT_NEGATIVE_INTEGER:
            lua_pushnumber(L, obj.via.i64);
            break;

        case MSGPACK_OBJECT_FLOAT32:
            lua_pushnumber(L, obj.via.f64);
            break;

        case MSGPACK_OBJECT_FLOAT64:
            lua_pushnumber(L, obj.via.f64);
            break;

        case MSGPACK_OBJECT_STR: {
            //std::string str(obj.via.str.ptr, obj.via.str.size);
            //std::wstring_convert<std::codecvt_utf8_utf16<char16_t>> converter;
            //std::wstring wstr = converter.from_bytes(str);
            //lua_pushlstring(L, (char*) wstr.c_str(), wstr.size() * 2);
            lua_pushlstring(L, obj.via.str.ptr, obj.via.str.size);
            break;
        }

        case MSGPACK_OBJECT_BIN: {
            // binary!
            char* blob = new char[BINARY_PREFIX_LEN+obj.via.str.size];
            strncpy(blob, BINARY_PREFIX, BINARY_PREFIX_LEN);
            strncpy(blob+BINARY_PREFIX_LEN, obj.via.str.ptr, obj.via.str.size);
            lua_pushlstring(L, blob, BINARY_PREFIX_LEN+obj.via.str.size);
            delete [] blob;
            break;
        }

        case MSGPACK_OBJECT_ARRAY:
            unpack_array(obj.via.array, L);
            break;

        case MSGPACK_OBJECT_MAP:
            unpack_map(obj.via.map, L);
            break;

        case MSGPACK_OBJECT_EXT:
        default:
            luaL_error(L, "invalid type: %d", obj.type);
            break;


    }

}

void gkvs::lua_ser::pack_table(lua_State* L, int index) {

    // check if this is an array
    // NOTE: This code strongly depends on the internal implementation
    // of Lua5.1. The table in Lua5.1 consists of two parts: the array part
    // and the hash part. The array part is placed before the hash part.
    // Therefore, it is possible to obtain the first key of the hash part
    // by using the return value of lua_objlen as the argument of lua_next.
    // If lua_next return 0, it means the table does not have the hash part,
    // that is, the table is an array.
    //
    // Due to the specification of Lua, the table with non-continous integral
    // keys is detected as a table, not an array.
    bool is_array = false;
    size_t len = lua_objlen(L, index);

    if (len > 0) {
        lua_pushnumber(L, len);
        if (lua_next(L, index) == 0) is_array = true;
        else lua_pop(L, 2);
    }

    if (is_array) {
        pack_array(L, index);
    }
    else {
        pack_map(L, index);
    }

}

void gkvs::lua_ser::pack_array(lua_State* L, int index) {

    int n = lua_gettop(L);
    size_t len = lua_objlen(L, index);

    msgpack_pack_array(&pk_, len);

    for (size_t i = 1; i <= len; i++) {
        lua_rawgeti(L, index, i);
        pack_obj(L, n + 1);
        lua_pop(L, 1);
    }

}

void gkvs::lua_ser::pack_map(lua_State* L, int index) {

    // calc size
    size_t len = 0;
    lua_pushnil(L);
    while (lua_next(L, index) != 0) {
        len++;
        lua_pop(L, 1);
    }

    msgpack_pack_map(&pk_, len);

    int n = lua_gettop(L); // used as a positive index
    lua_pushnil(L);
    while (lua_next(L, index) != 0) {
        pack_obj(L, n + 1); // -2:key
        pack_obj(L, n + 2); // -1:value
        lua_pop(L, 1); // removes value, keeps key for next iteration
    }

}


void gkvs::lua_ser::pack_obj(lua_State* L, int index) {

    if (!sbuf_free_) {
        msgpack_sbuffer_init(&sbuf_);
        sbuf_free_ = true;
        msgpack_packer_init(&pk_, &sbuf_, msgpack_sbuffer_write);
    }

    int type = lua_type(L, index);

    switch (type) {

        case LUA_TNIL:
            msgpack_pack_nil(&pk_);
            break;

        case LUA_TBOOLEAN: {
            int b = lua_toboolean(L, index);
            if (b != 0) {
                msgpack_pack_true(&pk_);
            }
            else {
                msgpack_pack_false(&pk_);
            }
            break;
        }

        case LUA_TNUMBER: {
            double d = lua_tonumber(L, index);
            int64_t n = lua_tointeger(L, index);

            if (static_cast<int64_t>(d) != n) {
                msgpack_pack_double(&pk_, d);
            }
            else {
                msgpack_pack_int64(&pk_, n);
            }
            break;
        }

        case LUA_TSTRING: {
            size_t len;
            const char* str = lua_tolstring(L, index, &len);
            if (str == nullptr) {
                luaL_error(L, "wrong string at index %d: type = %s", index, lua_typename(L, type));
                break;
            }

            if (len >= BINARY_PREFIX_LEN && !strncmp(BINARY_PREFIX, str, BINARY_PREFIX_LEN)) {
                // binary
                msgpack_pack_v4raw(&pk_, len - BINARY_PREFIX_LEN);
                msgpack_pack_v4raw_body(&pk_, str, len - BINARY_PREFIX_LEN);
            }
            else {
                // utf8
                msgpack_pack_str(&pk_, len);
                msgpack_pack_str_body(&pk_, str, len);
            }
            break;
        }

        case LUA_TTABLE:
            pack_table(L, index);
            break;

        case LUA_TUSERDATA:
            luaL_error(L, "userdata not supported: %s", lua_typename(L, type));
            break;

        case LUA_TFUNCTION:
        case LUA_TTHREAD:
        case LUA_TLIGHTUSERDATA:
        default:
            luaL_error(L, "invalid type: %s", lua_typename(L, type));
            break;
    }

}

void gkvs::lua_ser::print() {
    std::cout << "[";
    for (int i = 0; i < size(); ++i) {
        if (i != 0) {
            std::cout << ", ";
        }
        std::cout << (int) data()[i];
    }
    std::cout << "]" << std::endl;
}