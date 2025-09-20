
#ifndef RMDB_VALUE_H
#define RMDB_VALUE_H

#include "record/rm.h"

struct Value
{
    ColType type; // type of value
    union
    {
        int int_val;     // int value
        float float_val; // float value
    };
    std::string str_val; // string value

    std::shared_ptr<RmRecord> raw; // raw record buffer

    // Methods to set values
    void set_int(int int_val_)
    {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_)
    {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_)
    {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void init_raw(int len)
    {
        assert(!raw);
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT)
        {
            assert(len == sizeof(int));
            *reinterpret_cast<int *>(raw->data) = int_val;
        }
        else if (type == TYPE_FLOAT)
        {
            assert(len == sizeof(float));
            *reinterpret_cast<float *>(raw->data) = float_val;
        }
        else if (type == TYPE_STRING)
        {
            if (len < static_cast<int>(str_val.size()))
            {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            std::memcpy(raw->data, str_val.c_str(), str_val.size());
        }
    }

    // Equality operator
    bool operator==(const Value &other) const
    {
        if (type == other.type)
        {
            switch (type)
            {
            case TYPE_INT:
                return int_val == other.int_val;
            case TYPE_FLOAT:
                return float_val == other.float_val;
            case TYPE_STRING:
                return str_val == other.str_val;
            default:
                return false;
            }
        }
        else if (can_cast_type(other.type))
        {
            if (type == TYPE_INT)
                return static_cast<float>(int_val) == other.float_val;
            return float_val == static_cast<float>(other.int_val);
        }
        throw RMDBError("Type mismatch");
    }

    // Inequality operator
    bool operator!=(const Value &other) const { return !(*this == other); }

    // Less than operator
    bool operator<(const Value &other) const
    {
        if (type != other.type)
            throw RMDBError("Type mismatch");
        switch (type)
        {
        case TYPE_INT:
            return int_val < other.int_val;
        case TYPE_FLOAT:
            return float_val < other.float_val;
        case TYPE_STRING:
            return str_val < other.str_val;
        default:
            throw RMDBError("Invalid type for comparison");
        }
    }

    // Less than or equal operator
    bool operator<=(const Value &other) const { return *this < other || *this == other; }

    // Greater than operator
    bool operator>(const Value &other) const { return !(*this <= other); }

    // Greater than or equal operator
    bool operator>=(const Value &other) const { return !(*this < other); }

    // Method to check if type can be cast to another type
    bool can_cast_type(ColType to) const
    {
        if (type == to)
            return true;
        if (type == TYPE_INT && to == TYPE_FLOAT)
            return true;
        if (type == TYPE_FLOAT && to == TYPE_INT)
            return true;
        return false;
    }

    // Default constructor
    Value() = default;
};

namespace std
{
    template <>
    struct hash<Value>
    {
        std::size_t operator()(const Value &v) const
        {
            std::size_t h1 = std::hash<int>{}(static_cast<int>(v.type));
            std::size_t h2;
            switch (v.type)
            {
            case ColType::TYPE_INT:
                h2 = std::hash<int>{}(v.int_val);
                break;
            case ColType::TYPE_FLOAT:
                h2 = std::hash<float>{}(v.float_val);
                break;
            case ColType::TYPE_STRING:
                h2 = std::hash<std::string>{}(v.str_val);
                break;
            // add more cases if needed
            default:
                h2 = 0;
            }
            return h1 ^ (h2 << 1);
        }
    };
} // namespace std
#endif // RMDB_VALUE_H
