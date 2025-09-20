#pragma once

#include <iostream>
#include <map>
#include <string>
#include <type_traits>

// ColType 字段类型枚举
enum ColType
{
    TYPE_INT,   // 整型
    TYPE_FLOAT, // 浮点型
    TYPE_STRING // 字符串
};

inline std::string coltype2str(ColType type)
{
    std::map<ColType, std::string> m = {{TYPE_INT, "INT"}, {TYPE_FLOAT, "FLOAT"}, {TYPE_STRING, "STRING"}};
    return m.at(type);
}

class RecScan
{
public:
    virtual ~RecScan() = default;

    virtual void next() = 0;

    virtual bool is_end() const = 0;

    virtual char *rid() const = 0;
};
