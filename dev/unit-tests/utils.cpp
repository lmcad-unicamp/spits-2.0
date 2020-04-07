#include <arpa/inet.h>
#include <random>
#include <string>
#include "utils.hpp"

std::string generate_random_string(const int len) {
    char* s = new char[len+1];
    
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) 
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];

    s[len] = 0;
    
    std::string str(s);
    delete[] s;
    
    return str;
}


static uint64_t ntohll(uint64_t x)
{
    if (ntohs(1) == 1)
        return x;
    uint32_t lo = static_cast<uint32_t>((x & 0xFFFFFFFF00000000) >> 32);
    uint32_t hi = static_cast<uint32_t>(x & 0x00000000FFFFFFFF);
    uint64_t nlo = htonl(lo);
    uint64_t nhi = htonl(hi);
    return nhi << 32 | nlo;
}

static uint32_t get_4(unsigned char* data)
{
    uint32_t v = *((const uint32_t*)(data));
    return ntohl(v);
}

static uint64_t get_8(unsigned char* data)
{
    uint64_t v = *((const uint64_t*)(data));
    return ntohll(v);
}

int64_t read_longlong(unsigned char* data)
{
    uint64_t v = get_8(data);
    return *((int64_t*)(char*)&v);
}


float read_float(unsigned char* data)
{
    uint32_t v = get_4(data); 
    return *((float*)(char*)&v); 
}

double read_double(unsigned char* data) 
{ 
    uint64_t v = get_8(data); 
    return *((double*)(char*)&v); 
}

std::string read_string(unsigned char* data)
{
    std::string value;
    int i = 0;
    while (data[i] != '\0')
        value += data[i++];
    return value;
}
