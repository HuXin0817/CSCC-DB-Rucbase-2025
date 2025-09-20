#pragma once

#include "../defs.h"
#include "../errors.h"

/**
 * @brief 比较两个键值的辅助函数，根据类型进行比较
 *
 * @param a 第一个键值
 * @param b 第二个键值
 * @param type 键值的列类型
 * @param col_len 键值的长度
 * @return int 比较结果，a < b 返回-1，a > b 返回1，a == b 返回0
 */
inline int ix_compare(const char *a, const char *b, ColType type, int col_len)
{
    switch (type)
    {
    case TYPE_INT:
    {
        int ia = *reinterpret_cast<const int *>(a);
        int ib = *reinterpret_cast<const int *>(b);
        return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
    }
    case TYPE_FLOAT:
    {
        float fa = *reinterpret_cast<const float *>(a);
        float fb = *reinterpret_cast<const float *>(b);
        return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
    }
    case TYPE_STRING:
        return strncmp(a, b, col_len);
    default:
        throw InternalError("Unexpected data type");
    }
}

/**
 * @brief 比较两个键值的辅助函数，支持多列键的比较
 *
 * @param a 第一个键值
 * @param b 第二个键值
 * @param col_types 列类型的向量
 * @param col_lens 列长度的向量
 * @return int 比较结果，a < b 返回-1，a > b 返回1，a == b 返回0
 */
inline int ix_compare(const char *a, const char *b, const std::vector<ColType> &col_types, const std::vector<int> &col_lens)
{
    int offset = 0;
    for (size_t i = 0; i < col_types.size(); ++i)
    {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if (res != 0)
            return res;
        offset += col_lens[i];
    }
    return 0;
}

/**
 * @brief 比较两个键值的辅助函数，支持不同类型的键的比较
 *
 * @param a 第一个键值
 * @param b 第二个键值
 * @param type_1 第一个键值的类型
 * @param type_2 第二个键值的类型
 * @param col_len 键值的长度
 * @return int 比较结果，a < b 返回-1，a > b 返回1，a == b 返回0
 */
inline int ix_compare(const char *a, const char *b, ColType type_1, ColType type_2, int col_len)
{
    switch (type_1)
    {
    case TYPE_INT:
    {
        switch (type_2)
        {
        case TYPE_INT:
        {
            int ia = *reinterpret_cast<const int *>(a);
            int ib = *reinterpret_cast<const int *>(b);
            return ia < ib ? -1 : (ia > ib ? 1 : 0);
        }
        case TYPE_FLOAT:
        {
            int ia = *reinterpret_cast<const int *>(a);
            float fb = *reinterpret_cast<const float *>(b);
            return (ia < fb) ? -1 : (ia > fb ? 1 : 0);
        }
        case TYPE_STRING:
        {
            throw InternalError("Cannot compare integer with string");
        }
        default:
            throw InternalError("Unexpected data type");
        }
    }
    case TYPE_FLOAT:
    {
        switch (type_2)
        {
        case TYPE_INT:
        {
            float fa = *reinterpret_cast<const float *>(a);
            int ib = *reinterpret_cast<const int *>(b);
            return (fa < ib) ? -1 : ((fa > ib) ? 1 : 0);
        }
        case TYPE_FLOAT:
        {
            float fa = *reinterpret_cast<const float *>(a);
            float fb = *reinterpret_cast<const float *>(b);
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case TYPE_STRING:
        {
            throw InternalError("Cannot compare float with string");
        }
        default:
            throw InternalError("Unexpected data type");
        }
    }
    case TYPE_STRING:
    {
        int result = memcmp(a, b, col_len);
        return (result < 0) ? -1 : ((result > 0) ? 1 : 0);
    }
    default:
        throw InternalError("Unexpected data type");
    }
}
