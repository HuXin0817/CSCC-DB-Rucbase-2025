#pragma once

#include "common/common_finals.h"

class AbstractExecutor
{
public:
    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const
    {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    }

    virtual void beginTuple() {};

    virtual void nextTuple() {};

    virtual bool is_end() const { return true; };

    virtual char *rid() const { return nullptr; }

    virtual std::unique_ptr<RmRecord> Next() { return nullptr; }

protected:
    static bool can_cast_type(ColType from, ColType to)
    {
        if (from == to)
            return true;
        if (from == TYPE_INT && to == TYPE_FLOAT)
            return true;
        if (from == TYPE_FLOAT && to == TYPE_INT)
            return true;
        return false;
    }

    static void cast_value(Value &val, ColType to)
    {
        // Add logic to cast val to the target type
        if (val.type == TYPE_INT && to == TYPE_FLOAT)
        {
            int int_val = val.int_val;
            val.type = TYPE_FLOAT;
            val.float_val = static_cast<float>(int_val);
        }
        else if (val.type == TYPE_FLOAT && to == TYPE_INT)
        {
            float float_val = val.float_val;
            val.type = TYPE_INT;
            val.int_val = static_cast<int>(float_val);
        }
        else
        {
            throw RMDBError();
        }
    }

    static std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target)
    {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col)
                                { return col.tab_name == target.tab_name && col.name == target.col_name; });
        if (pos == rec_cols.end())
        {
            throw RMDBError();
        }
        return pos;
    }
};
