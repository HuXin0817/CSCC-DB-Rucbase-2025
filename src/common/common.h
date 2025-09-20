#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_set>
#include <variant>
#include <vector>

#include "Value.h"
#include "defs.h"
#include "parser/ast.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

struct TabCol
{
    std::string tab_name;
    std::string col_name;

    std::string alias;

    ast::AggFuncType aggFuncType;

    friend bool operator<(const TabCol &x, const TabCol &y) { return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name); }
    bool empty() const { return tab_name.empty() && col_name.empty(); }
};

class Query;
class Plan;
class PortalStmt;
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
    OP_NE,
    OP_LT,
    OP_GT,
    OP_LE,
    OP_GE,
    OP_IN,
    OP_NOT_IN
};

struct Condition
{
    TabCol lhs_col; // left-hand side column
    std::vector<ColMeta>::iterator lhs;
    CompOp op;       // comparison operator
    bool is_rhs_val; // true if right-hand side is a value (not a column)
    TabCol rhs_col;  // right-hand side column
    std::vector<ColMeta>::iterator rhs;
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
    TabCol lhs;
    Value rhs;
};
