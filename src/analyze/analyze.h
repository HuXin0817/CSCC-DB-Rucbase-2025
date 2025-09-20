#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/common.h"
#include "parser/parser.h"
#include "system/sm.h"

class Query
{
public:
    std::shared_ptr<ast::TreeNode> parse;
    // TODO jointree
    // where条件
    std::vector<Condition> conds;
    // 投影列
    std::vector<TabCol> cols;
    // 表名
    std::vector<std::string> tables;
    // update 的set 值
    std::vector<SetClause> set_clauses;
    // insert 的values值
    std::vector<Value> values;

    std::vector<HavingCond> having_conds;

    Query() = default;
};

class Analyze
{
private:
    SmManager *sm_manager_;

public:
    explicit Analyze(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    ~Analyze() = default;

    std::shared_ptr<Query> do_analyze(std::shared_ptr<ast::TreeNode> root);

private:
    static TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target);

    void get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols);

    void get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds);

    void check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds);

    Value convert_sv_value(const std::shared_ptr<ast::Value> &sv_val);

    CompOp convert_sv_comp_op(ast::SvCompOp op);

    static bool can_cast_type(ColType from, ColType to);

    static void cast_value(Value &val, ColType to);

    void get_having(std::shared_ptr<ast::GroupBy> &group_by, std::vector<HavingCond> &having_conds, const std::string &table_name, const std::vector<TabCol> &select_cols);
};
