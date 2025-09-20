#pragma once

#include <assert.h>

#include <cstring>
#include <iostream>
#include <map>
#include <type_traits>
#include <vector>

namespace encode
{

    static size_t serialize(const char *str, char *dest, size_t size)
    {
        std::memcpy(dest, str, size);
        return size;
    }

    static size_t deserialize(char *&str, const char *dest, size_t size)
    {
        if (dest == nullptr)
        {
            return 0;
        }
        if (size == 0)
        {
            return 0;
        }
        str = new char[size + 1];
        std::memcpy(str, dest, size);
        str[size] = '\0';
        return size;
    }

    static size_t serialize(const char *str, char *dest)
    {
        auto offset = strlen(str);
        std::memcpy(dest, str, strlen(str));
        return offset;
    }

    static size_t deserialize(const char *str, [[maybe_unused]] char *dest) { return strlen(str); }

    static size_t deserialize(const char *str, [[maybe_unused]] const char *dest) { return strlen(str); }

    static size_t serialize(const int &num, char *dest)
    {
        std::string num_str = "\"" + std::to_string(num) + "\"";
        auto len = num_str.length();
        std::memcpy(dest, num_str.c_str(), len);
        return len;
    }

    static size_t deserialize(int &num, const char *src)
    {
        assert(*src == '"');

        size_t pos = 1;
        while (src[pos] != '"')
        {
            pos++;
        }

        std::string num_str(src + 1, pos - 1);
        num = std::stoi(num_str);
        return num_str.length() + 2;
    }

    static size_t serialize(const size_t &num, char *dest)
    {
        std::string num_str = "\"" + std::to_string(num) + "\"";
        auto len = num_str.length();
        std::memcpy(dest, num_str.c_str(), len);
        return len;
    }

    static size_t deserialize(size_t &num, const char *src)
    {
        assert(*src == '"');

        size_t pos = 1;
        while (src[pos] != '"')
        {
            pos++;
        }

        std::string num_str(src + 1, pos - 1);
        num = std::stol(num_str);
        return num_str.length() + 2;
    }

} // namespace encode
