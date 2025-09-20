#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "defs_finals.h"
#include "parser/ast.h"
#include "record/rm_defs_finals.h"
#include "value_finals.h"

struct TabCol
{
    std::string tab_name;
    std::string col_name;

    std::string alias;

    ast::AggFuncType aggFuncType;

    friend bool operator<(const TabCol &x, const TabCol &y)
    {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }

    bool empty() const
    {
        return tab_name.empty() && col_name.empty();
    }
};

class Query;

class Plan;

struct PortalStmt;

struct SubQuery
{
    std::shared_ptr<ast::SelectStmt> stmt;
    std::shared_ptr<Query> query;
    std::shared_ptr<Plan> plan;
    std::shared_ptr<PortalStmt> portalStmt;

    bool is_scalar = false;

    ColType subquery_type;
    std::unordered_set<Value> result;
};

enum CompOp
{
    OP_EQ,
    OP_LT,
    OP_GT,
    OP_LE,
    OP_GE,
};

enum UpdateOp
{
    SELF_ADD = 0,
    SELF_SUB,
    SELF_MUT,
    SELF_DIV,
    ASSINGMENT,
    UNKNOWN
};

struct Condition
{
    TabCol lhs_col; // left-hand side column
    ColMeta lhs;
    CompOp op;       // comparison operator
    bool is_rhs_val; // true if right-hand side is a value (not a column)
    TabCol rhs_col;  // right-hand side column
    ColMeta rhs;
    Value rhs_val; // right-hand side value

    // subquery
    bool is_subquery = false;
    std::shared_ptr<SubQuery> subQuery;

    bool join_cond = false;
};

struct HavingCond
{
    TabCol lhs_col; // left-hand side column
    CompOp op;      // comparison operator
    Value rhs_val;  // right-hand side value
};

struct SetClause
{
    // TabCol lhs;

    std::shared_ptr<ColMeta> lhs;

    UpdateOp op;

    Value rhs;

    SetClause(Value rhs_)
    {
        rhs = std::move(rhs_);
        op = UpdateOp::UNKNOWN;
    }

    void set_op(int op_)
    {
        switch (op_)
        {
        case 0:
            op = UpdateOp::SELF_ADD;
            break;
        case 1:
            op = UpdateOp::SELF_SUB;
            break;
        case 2:
            op = UpdateOp::SELF_MUT;
            break;
        case 3:
            op = UpdateOp::SELF_DIV;
            break;
        case 4:
            op = UpdateOp::SELF_ADD;
            break;
        case 5:
            op = UpdateOp::ASSINGMENT;
            break;
        default:
            throw RMDBError();
        }
    }
};
