/*
 * =====================================================================================
 *
 *      Filename:  pb.c
 *
 *      Description: protoc-gen-lua
 *      Google's Protocol Buffers project, ported to lua.
 *      https://code.google.com/p/protoc-gen-lua/
 *
 *      Copyright (c) 2010 , 林卓毅 (Zhuoyi Lin) netsnail@gmail.com
 *      All rights reserved.
 *
 *      Use, modification and distribution are subject to the "New BSD License"
 *      as listed at <url: http://www.opensource.org/licenses/bsd-license.php >.
 *
 *      Created:  2010年08月02日 18时04分21秒
 *
 *      Company:  NetEase
 *
 * =====================================================================================
 */
#include <stdint.h>
#include <string.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#ifdef _ALLBSD_SOURCE
#include <machine/endian.h>
#else
#include <endian.h>
#endif

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define IS_LITTLE_ENDIAN
#endif

#define IOSTRING_META "protobuf.IOString"

#define checkiostring(L) \
    (IOString*) luaL_checkudata(L, 1, IOSTRING_META)

#define IOSTRING_BUF_LEN 65535

typedef struct{
    size_t size;
    char buf[IOSTRING_BUF_LEN];
} IOString;

// !! str should has enough memery
// @return result string length
static int convert_int64_to_str(int64_t n64,char* str)
{
    assert(str != NULL);
    int len = 0;
    int64_t num = n64;
    bool neg = false;
    if (num < 0)
    {
        num = -num;
        neg = true;
    }
    if (num == 0)
    {
        str[len++] = '0';
    }
    while (num > 0)
    {
        str[len++] = num % 10 + '0';
        num /= 10;
    }
    if (neg)
    {
        str[len++] = '-';
    }

    for (int i = 0, tl = len / 2; i < tl; i++)
    {
        char t = str[i];
        str[i] = str[len - i - 1];
        str[len - i - 1] = t;
    }
    
    str[len] = '\0';
    return len;
}

static int64_t convert_str_to_int64(const char* str,int len)
{
    assert(str != NULL);
    int64_t num = 0;
    int neg = 1;
    int idx = 0;
    if (len > 0 && str[0] == '-')
    {
        neg = -1;
        idx = 1;
    }
    for (; (idx < len) && str[idx]; ++idx)
    {
        num = num * 10 + (str[idx] - '0');
    }
    num *= neg;
    return num;
}

static int64_t get_int64_from_stack(lua_State *L, int idx)
{
    size_t len;
    const char* buffer = luaL_checklstring(L, idx, &len);

    return convert_str_to_int64(buffer, len);
}

static void pack_varint(luaL_Buffer *b, uint64_t value)
{
    if (value >= 0x80)
    {
        luaL_addchar(b, value | 0x80);
        value >>= 7;
        if (value >= 0x80)
        {
            luaL_addchar(b, value | 0x80);
            value >>= 7;
            if (value >= 0x80)
            {
                luaL_addchar(b, value | 0x80);
                value >>= 7;
                if (value >= 0x80)
                {
                    luaL_addchar(b, value | 0x80);
                    value >>= 7;
                    if (value >= 0x80)
                    {
                        luaL_addchar(b, value | 0x80);
                        value >>= 7;
                        if (value >= 0x80)
                        {
                            luaL_addchar(b, value | 0x80);
                            value >>= 7;
                            if (value >= 0x80)
                            {
                                luaL_addchar(b, value | 0x80);
                                value >>= 7;
                                if (value >= 0x80)
                                {
                                    luaL_addchar(b, value | 0x80);
                                    value >>= 7;
                                    if (value >= 0x80)
                                    {
                                        luaL_addchar(b, value | 0x80);
                                        value >>= 7;
                                    } 
                                } 
                            } 
                        } 
                    } 
                } 
            } 
        } 
    }
    luaL_addchar(b, value);
} 

static int varint_encoder(lua_State *L)
{
    lua_Number l_value = luaL_checknumber(L, 2);
    uint64_t value = (uint64_t)l_value;

    luaL_Buffer b;
    luaL_buffinit(L, &b);

    pack_varint(&b, value);

    lua_settop(L, 1);
    luaL_pushresult(&b);
    lua_call(L, 1, 0);
    return 0;
}

static int signed_varint_encoder(lua_State *L)
{
    lua_Number l_value = luaL_checknumber(L, 2);
    int64_t value = (int64_t)l_value;

    luaL_Buffer b;
    luaL_buffinit(L, &b);

    if (value < 0)
    {
        pack_varint(&b, *(uint64_t*)&value);
    }else{
        pack_varint(&b, value);
    }

    lua_settop(L, 1);
    luaL_pushresult(&b);
    lua_call(L, 1, 0);
    return 0;
}

static int signed_varint_encoder64(lua_State *L)
{
    int64_t value = get_int64_from_stack(L, 2);

    luaL_Buffer b;
    luaL_buffinit(L, &b);

    if (value < 0)
    {
        pack_varint(&b, *(uint64_t*)&value);
    }
    else{
        pack_varint(&b, value);
    }

    lua_settop(L, 1);
    luaL_pushresult(&b);
    lua_call(L, 1, 0);
    return 0;
}

static int pack_fixed32(lua_State *L, uint8_t* value){
#ifdef IS_LITTLE_ENDIAN
    lua_pushlstring(L, (char*)value, 4);
#else
    uint32_t v = htole32(*(uint32_t*)value);
    lua_pushlstring(L, (char*)&v, 4);
#endif
    return 0;
}

static int pack_fixed64(lua_State *L, uint8_t* value){
#ifdef IS_LITTLE_ENDIAN
    lua_pushlstring(L, (char*)value, 8);
#else
    uint64_t v = htole64(*(uint64_t*)value);
    lua_pushlstring(L, (char*)&v, 8);
#endif
    return 0;
}

static int struct_pack(lua_State *L)
{
    uint8_t format = luaL_checkinteger(L, 2);
    lua_Number value = luaL_checknumber(L, 3);
    lua_settop(L, 1);

    switch(format){
        case 'i':
            {
                int32_t v = (int32_t)value;
                pack_fixed32(L, (uint8_t*)&v);
                break;
            }
        case 'q':
            {
                int64_t v = (int64_t)value;
                pack_fixed64(L, (uint8_t*)&v);
                break;
            }
        case 'f':
            {
                float v = (float)value;
                pack_fixed32(L, (uint8_t*)&v);
                break;
            }
        case 'd':
            {
                double v = (double)value;
                pack_fixed64(L, (uint8_t*)&v);
                break;
            }
        case 'I':
            {
                uint32_t v = (uint32_t)value;
                pack_fixed32(L, (uint8_t*)&v);
                break;
            }
        case 'Q':
            {
                uint64_t v = (uint64_t) value;
                pack_fixed64(L, (uint8_t*)&v);
                break;
            }
        default:
            luaL_error(L, "Unknown, format");
    }
    lua_call(L, 1, 0);
    return 0;
}

static size_t size_varint(const char* buffer, size_t len)
{
    size_t pos = 0;
    while(buffer[pos] & 0x80){
        ++pos;
        if(pos > len){
            return -1;
        }
    }
    return pos+1;
}

static uint64_t unpack_varint(const char* buffer, size_t len)
{
    uint64_t value = buffer[0] & 0x7f;
    size_t shift = 7;
    size_t pos=0;
    for(pos = 1; pos < len; ++pos)
    {
        value |= ((uint64_t)(buffer[pos] & 0x7f)) << shift;
        shift += 7;
    }
    return value;
}

static int varint_decoder(lua_State *L)
{
    size_t len;
    const char* buffer = luaL_checklstring(L, 1, &len);
    size_t pos = luaL_checkinteger(L, 2);
    
    buffer += pos;
    len = size_varint(buffer, len);
    if(len == -1){
        luaL_error(L, "error data %s, len:%d", buffer, len);
    }else{
        lua_pushnumber(L, (lua_Number)unpack_varint(buffer, len));
        lua_pushinteger(L, len + pos);
    }
    return 2;
}

static int signed_varint_decoder(lua_State *L)
{
    size_t len;
    const char* buffer = luaL_checklstring(L, 1, &len);
    size_t pos = luaL_checkinteger(L, 2);
    buffer += pos;
    len = size_varint(buffer, len);
    
    if(len == -1){
        luaL_error(L, "error data %s, len:%d", buffer, len);
    }else{
        lua_pushnumber(L, (lua_Number)(int64_t)unpack_varint(buffer, len));
        lua_pushinteger(L, len + pos);
    }
    return 2;
}

static int signed_varint_decoder64(lua_State *L)
{
    size_t len;
    const char* buffer = luaL_checklstring(L, 1, &len);
    size_t pos = luaL_checkinteger(L, 2);
    buffer += pos;
    len = size_varint(buffer, len);

    if (len == -1){
        luaL_error(L, "error data %s, len:%d", buffer, len);
    }
    else{
        int64_t num = unpack_varint(buffer, len);
        char buffer[32] = {0};
        convert_int64_to_str(num, buffer);
        lua_pushstring(L, (const char*)buffer);
        lua_pushinteger(L, len + pos);
    }
    return 2;
}

static int zigint_decoder64(lua_State *L)
{
    size_t len;
    const char* buffer = luaL_checklstring(L, 1, &len);
    size_t pos = luaL_checkinteger(L, 2);
    buffer += pos;
    len = size_varint(buffer, len);

    if (len == -1){
        luaL_error(L, "error data %s, len:%d", buffer, len);
    }
    else{
        uint64_t n = unpack_varint(buffer, len);
        int64_t value = (n >> 1) ^ -(int64_t)(n & 1);
        char buffer[32] = { 0 };
        convert_int64_to_str(value, buffer);
        lua_pushstring(L, (const char*)buffer);
        lua_pushinteger(L, len + pos);
    }
    return 2;
}

static int zig_zag_encode32(lua_State *L)
{
    int32_t n = luaL_checkinteger(L, 1);
    uint32_t value = (n << 1) ^ (n >> 31);
    lua_pushinteger(L, value);
    return 1;
}

static int zig_zag_decode32(lua_State *L)
{
    uint32_t n = (uint32_t)luaL_checkinteger(L, 1);
    int32_t value = (n >> 1) ^ - (int32_t)(n & 1);
    lua_pushinteger(L, value);
    return 1;
}

static int zig_zag_encode64(lua_State *L)
{
    int64_t n = (int64_t)luaL_checknumber(L, 1);
    uint64_t value = (n << 1) ^ (n >> 63);
    lua_pushinteger(L, value);
    return 1;
}

static int zig_zag_decode64(lua_State *L)
{
    uint64_t n = (uint64_t)luaL_checknumber(L, 1);
    int64_t value = (n >> 1) ^ - (int64_t)(n & 1);
    lua_pushinteger(L, value);
    return 1;
}

static int read_tag(lua_State *L)
{
    size_t len;
    const char* buffer = luaL_checklstring(L, 1, &len);
    size_t pos = luaL_checkinteger(L, 2);
    
    buffer += pos;
    len = size_varint(buffer, len);
    if(len == -1){
        luaL_error(L, "error data %s, len:%d", buffer, len);
    }else{
        lua_pushlstring(L, buffer, len);
        lua_pushinteger(L, len + pos);
    }
    return 2;
}

static const uint8_t* unpack_fixed32(const uint8_t* buffer, uint8_t* cache)
{
#ifdef IS_LITTLE_ENDIAN
    return buffer;
#else
    *(uint32_t*)cache = le32toh(*(uint32_t*)buffer);
    return cache;
#endif
}

static const uint8_t* unpack_fixed64(const uint8_t* buffer, uint8_t* cache)
{
#ifdef IS_LITTLE_ENDIAN
    return buffer;
#else
    *(uint64_t*)cache = le64toh(*(uint64_t*)buffer);
    return cache;
#endif
}

static int struct_unpack(lua_State *L)
{
    uint8_t format = luaL_checkinteger(L, 1);
    size_t len;
    const uint8_t* buffer = (uint8_t*)luaL_checklstring(L, 2, &len);
    size_t pos = luaL_checkinteger(L, 3);

    buffer += pos;
    uint8_t out[8];
    switch(format){
        case 'i':
            {
                lua_pushinteger(L, *(int32_t*)unpack_fixed32(buffer, out));
                break;
            }
        case 'q':
            {
                lua_pushnumber(L, (lua_Number)*(int64_t*)unpack_fixed64(buffer, out));
                break;
            }
        case 'f':
            {
                lua_pushnumber(L, (lua_Number)*(float*)unpack_fixed32(buffer, out));
                break;
            }
        case 'd':
            {
                lua_pushnumber(L, (lua_Number)*(double*)unpack_fixed64(buffer, out));
                break;
            }
        case 'I':
            {
                lua_pushnumber(L, *(uint32_t*)unpack_fixed32(buffer, out));
                break;
            }
        case 'Q':
            {
                lua_pushnumber(L, (lua_Number)*(uint64_t*)unpack_fixed64(buffer, out));
                break;
            }
        default:
            luaL_error(L, "Unknown, format");
    }
    return 1;
}

static int iostring_new(lua_State* L)
{
    IOString* io = (IOString*)lua_newuserdata(L, sizeof(IOString));
    io->size = 0;

    luaL_getmetatable(L, IOSTRING_META);
    lua_setmetatable(L, -2); 
    return 1;
}

static int iostring_str(lua_State* L)
{
    IOString *io = checkiostring(L);
    lua_pushlstring(L, io->buf, io->size);
    return 1;
}

static int iostring_len(lua_State* L)
{
    IOString *io = checkiostring(L);
    lua_pushinteger(L, io->size);
    return 1;
}

static int iostring_write(lua_State* L)
{
    IOString *io = checkiostring(L);
    size_t size;
    const char* str = luaL_checklstring(L, 2, &size);
    if(io->size + size > IOSTRING_BUF_LEN){
        luaL_error(L, "Out of range");
    }
    memcpy(io->buf + io->size, str, size);
    io->size += size;
    return 0;
}

static int iostring_sub(lua_State* L)
{
    IOString *io = checkiostring(L);
    size_t begin = luaL_checkinteger(L, 2);
    size_t end = luaL_checkinteger(L, 3);

    if(begin > end || end > io->size)
    {
        luaL_error(L, "Out of range");
    }
    lua_pushlstring(L, io->buf + begin - 1, end - begin + 1);
    return 1;
}

static int iostring_clear(lua_State* L)
{
    IOString *io = checkiostring(L);
    io->size = 0; 
    return 0;
}

static int _varint_save_size(uint64_t u64)
{
    int count = 1;
    while (u64 >= 0x80)
    {
        ++count;
        u64 >>= 7;
    }
    return count;
}

static int varint_save_size64(lua_State* L)
{
    int64_t num = get_int64_from_stack(L, 1);
    int count = _varint_save_size(num);
    lua_pushinteger(L, count);
    return 1;
}

static int zigint_save_size64(lua_State* L)
{
    int64_t num = get_int64_from_stack(L, 1);
    uint64_t zig = (num << 1) ^ (num >> 63);
    lua_Number count = _varint_save_size(zig);
    lua_pushinteger(L, count);
    return 1;
}

static int zigint_encoder64(lua_State *L)
{
    int64_t l_value = get_int64_from_stack(L, 2);
    uint64_t value = (l_value << 1) ^ (l_value >> 63);

    luaL_Buffer b;
    luaL_buffinit(L, &b);
    pack_varint(&b, value);

    lua_settop(L, 1);
    luaL_pushresult(&b);
    lua_call(L, 1, 0);
    return 0;
}

static const struct luaL_reg _pb [] = {
    {"varint_encoder", varint_encoder},
    {"signed_varint_encoder", signed_varint_encoder},
    { "signed_varint_encoder64", signed_varint_encoder64 },
    { "zigint_encoder64", zigint_encoder64 },
    {"read_tag", read_tag},
    {"struct_pack", struct_pack},
    {"struct_unpack", struct_unpack},
    {"varint_decoder", varint_decoder},
    {"signed_varint_decoder", signed_varint_decoder},
    { "signed_varint_decoder64", signed_varint_decoder64 },
    { "zigint_decoder64", zigint_decoder64 },
    {"zig_zag_decode32", zig_zag_decode32},
    {"zig_zag_encode32", zig_zag_encode32},
    {"zig_zag_decode64", zig_zag_decode64},
    {"zig_zag_encode64", zig_zag_encode64 },
    {"new_iostring", iostring_new},
    { "varint_save_size64", varint_save_size64 },
    { "zigint_save_size64", zigint_save_size64 },
    {NULL, NULL}
};

static const struct luaL_reg _c_iostring_m [] = {
    {"__tostring", iostring_str},
    {"__len", iostring_len},
    {"write", iostring_write},
    {"sub", iostring_sub},
    {"clear", iostring_clear},
    {NULL, NULL}
};

int luaopen_pb (lua_State *L)
{
    luaL_newmetatable(L, IOSTRING_META);
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    luaL_register(L, NULL, _c_iostring_m);

    luaL_register(L, "pb", _pb);
    return 1;
} 
