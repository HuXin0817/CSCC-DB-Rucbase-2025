#pragma once

#include <iostream>
#include <map>
#include <string>
#include <type_traits>

#include "common/encode.h"

#define new_char(size) (std::make_unique<char[]>(size))

// 此处重载了<<操作符，在ColMeta中进行了调用
/**
 * @brief 输出枚举值
 *
 * @tparam T 枚举类型
 * @param os 输出流
 * @param enum_val 枚举值
 * @return std::ostream& 输出流
 */
template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val)
{
    os << static_cast<int>(enum_val);
    return os;
}

/**
 * @brief 输入枚举值
 *
 * @tparam T 枚举类型
 * @param is 输入流
 * @param enum_val 枚举值
 * @return std::istream& 输入流
 */
template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val)
{
    int int_val;
    is >> int_val;
    enum_val = static_cast<T>(int_val);
    return is;
}

/**
 * @brief 记录标识符结构
 */
struct Rid
{
    int page_no = -1; // 页位置
    int slot_no = -1; // 槽位置

    /**
     * @brief 比较两个 Rid 是否相等
     *
     * @param x 第一个 Rid
     * @param y 第二个 Rid
     * @return true 如果相等
     * @return false 如果不等
     */
    friend bool operator==(const Rid &x, const Rid &y) { return x.page_no == y.page_no && x.slot_no == y.slot_no; }

    /**
     * @brief 比较两个 Rid 是否不等
     *
     * @param x 第一个 Rid
     * @param y 第二个 Rid
     * @return true 如果不等
     * @return false 如果相等
     */
    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }

    friend bool operator<(const Rid &x, const Rid &y)
    {
        if (x.page_no != y.page_no)
        {
            return x.page_no < y.page_no;
        }

        return x.slot_no < y.slot_no;
    }

    std::string to_string() const { return "{page_no: " + std::to_string(page_no) + ", slot_no: " + std::to_string(slot_no) + "}"; }

    size_t serialize(char *dest) const
    {
        size_t offset = 0;
        offset += encode::serialize("Rid{page_no: ", dest + offset);
        offset += encode::serialize(page_no, dest + offset);
        offset += encode::serialize(", slot_no: ", dest + offset);
        offset += encode::serialize(slot_no, dest + offset);
        offset += encode::serialize("}", dest + offset);
        return offset;
    }

    size_t deserialize(const char *dest)
    {
        size_t offset = 0;
        offset += encode::deserialize("Rid{page_no: ", dest + offset);
        offset += encode::deserialize(page_no, dest + offset);
        offset += encode::deserialize(", slot_no: ", dest + offset);
        offset += encode::deserialize(slot_no, dest + offset);
        offset += encode::deserialize("}", dest + offset);
        return offset;
    }
};

// ColType 字段类型枚举
enum ColType
{
    TYPE_INT,     // 整型
    TYPE_FLOAT,   // 浮点型
    TYPE_STRING,  // 字符串
    TYPE_DATETIME // 日期
};

/**
 * @brief 将 ColType 转换为字符串
 *
 * @param type 字段类型
 * @return std::string 字符串表示
 */
inline std::string coltype2str(ColType type)
{
    std::map<ColType, std::string> m = {{TYPE_INT, "INT"}, {TYPE_FLOAT, "FLOAT"}, {TYPE_STRING, "STRING"}, {TYPE_DATETIME, "DATETIME"}};
    return m.at(type);
}

/**
 * @brief 将字符串转换为 ColType
 *
 * @param str 字符串表示
 * @return ColType 字段类型
 */
inline ColType str2coltype(const std::string &str)
{
    static std::map<std::string, ColType> m = {{"INT", TYPE_INT}, {"FLOAT", TYPE_FLOAT}, {"STRING", TYPE_STRING}, {"DATETIME", TYPE_DATETIME}};
    return m.at(str);
}

/**
 * @brief 抽象记录扫描类
 */
class RecScan
{
public:
    virtual ~RecScan() = default;

    /**
     * @brief 移动到下一个记录
     */
    virtual void next() = 0;

    /**
     * @brief 检查是否到达扫描结束
     *
     * @return true 如果到达结束
     * @return false 如果未结束
     */
    virtual bool is_end() const = 0;

    /**
     * @brief 获取当前记录的 Rid
     *
     * @return Rid 当前记录的 Rid
     */
    virtual Rid rid() const = 0;
};
