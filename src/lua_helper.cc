
#include <codecvt>
#include <iconv.h>

#include "lua_helper.h"


#include <glog/logging.h>
#include <msgpack/object.h>

#define BINARY_PREFIX "binary!"
#define BINARY_PREFIX_LEN 7

#if LUA_VERSION_NUM > 501
    #define lua_objlen lua_rawlen
#endif


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

    sbuf_pos_ = sbuf_.size;

    int type = lua_type(L, index);

    switch (type) {

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

        case LUA_TNIL:
        case LUA_TTHREAD:
        case LUA_TLIGHTUSERDATA:
        default:
            luaL_error(L, "invalid type: %s", lua_typename(L, type));
            break;
    }

}