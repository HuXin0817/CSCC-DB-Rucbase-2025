#pragma once

#include "common/common.h"
#include "execution_defs.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor
{
public:
    Rid _abstract_rid{};

    Context *context_{};

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const
    {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual void beginTuple() {};

    virtual void nextTuple() {};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target)
    {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col)
                                { return col.tab_name == target.tab_name && col.name == target.col_name; });
        if (pos == rec_cols.end())
        {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

protected:
    bool compare(const std::unique_ptr<RmRecord> &record, const Condition &cond) const
    {
        char *lhs_buf = record->data + cond.lhs->offset;
        char *rhs_buf = nullptr;

        if (!cond.is_rhs_val)
        {
            rhs_buf = record->data + cond.rhs->offset;
        }

        switch (cond.lhs->type)
        {
        case TYPE_INT:
        {
            int lhs_value = *reinterpret_cast<int *>(lhs_buf);
            if (cond.rhs_val.type == TYPE_FLOAT)
            {
                return compare_values((float)lhs_value, cond.rhs_val.float_val, cond.op);
            }
            return compare_values(lhs_value, cond.rhs_val.int_val, cond.op);
        }
        case TYPE_FLOAT:
        {
            float lhs_value = *reinterpret_cast<float *>(lhs_buf);
            float rhs_value = cond.is_rhs_val ? cond.rhs_val.float_val : *reinterpret_cast<float *>(rhs_buf);
            return compare_values(lhs_value, rhs_value, cond.op);
        }
        case TYPE_STRING:
        {
            // Use std::string::compare for string comparison
            std::string lhs_value(lhs_buf, strnlen(lhs_buf, cond.lhs->len));
            std::string rhs_value = cond.is_rhs_val ? std::string(cond.rhs_val.str_val) : std::string(rhs_buf, strnlen(rhs_buf, cond.rhs->len));
            int cmp_result = lhs_value.compare(rhs_value);
            return compare_result(cmp_result, cond.op);
        }
        }
        return false;
    }

    template <typename T>
    bool compare_values(const T &lhs, const T &rhs, CompOp op) const
    {
        return compare_result(lhs - rhs, op);
    }

    bool compare_result(double cmp_result, CompOp op) const
    {
        switch (op)
        {
        case OP_EQ:
            return cmp_result == 0;
        case OP_NE:
            return cmp_result != 0;
        case OP_LT:
            return cmp_result < 0;
        case OP_GT:
            return cmp_result > 0;
        case OP_LE:
            return cmp_result <= 0;
        case OP_GE:
            return cmp_result >= 0;
        default:
            return false;
        }
    }

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
            throw IncompatibleTypeError(coltype2str(val.type), coltype2str(to));
        }
    }

    size_t strnlen(const char *s, size_t max_len) const
    {
        size_t len = 0;
        while (len < max_len && s[len])
            ++len;
        return len;
    }

    bool compare(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, std::unique_ptr<RmRecord> &rec)
    {
        return std::all_of(conds.begin(), conds.end(), [&](const Condition &cond)
                           {
            auto lhs_col = get_col(rec_cols, cond.lhs_col);
            auto lhs = rec->data + lhs_col->offset;
            char *rhs;
            ColType rhs_type;
            if (cond.is_rhs_val) {
                rhs_type = cond.rhs_val.type;
                rhs = cond.rhs_val.raw->data;
            } else {
                auto rhs_col = get_col(rec_cols, cond.rhs_col);
                rhs_type = rhs_col->type;
                rhs = rec->data + rhs_col->offset;
            }
            assert(rhs_type == lhs_col->type);
            auto cmp = ix_compare(lhs, rhs, rhs_type, lhs_col->len);

            switch (cond.op) {
                case OP_EQ:
                    return cmp == 0;
                case OP_NE:
                    return cmp != 0;
                case OP_LT:
                    return cmp < 0;
                case OP_GT:
                    return cmp > 0;
                case OP_LE:
                    return cmp <= 0;
                case OP_GE:
                    return cmp >= 0;
                default:
                    throw InternalError("Unexpected op type");
            } });
    }

    bool compare(const Value &value1, const Value &value2) const
    {
        switch (value1.type)
        {
        case TYPE_INT:
        {
            if (value2.type == TYPE_INT)
            {
                return value1.int_val > value2.int_val;
            }
            else if (value2.type == TYPE_FLOAT)
            {
                return value1.int_val > value2.float_val;
            }
        }
        case TYPE_FLOAT:
        {
            if (value2.type == TYPE_INT)
            {
                return value1.float_val > value2.int_val;
            }
            else if (value2.type == TYPE_FLOAT)
            {
                return value1.float_val > value2.float_val;
            }
        }
        case TYPE_STRING:
            return strcmp(value1.str_val.c_str(), value2.str_val.c_str()) > 0 ? true : false;
        }
        throw InternalError("Type_Not Find");
    }
};
