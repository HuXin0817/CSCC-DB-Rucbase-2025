#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/ast.h"
#include "parser/parser.h"
#include "system/sm_manager_finals.h"

typedef enum PlanTag
{
    T_Invalid = 1,
    T_Help,
    T_ShowTable,
    T_DescTable,
    T_DescIndex,
    T_CreateTable,
    T_DropTable,
    T_CreateIndex,
    T_DropIndex,
    T_SetKnob,
    T_Insert,
    T_Update,
    T_Delete,
    T_select,
    T_Transaction_begin,
    T_Transaction_commit,
    T_Transaction_abort,
    T_Transaction_rollback,
    T_SeqScan,
    T_IndexScan,
    T_NestLoop,
    T_SortMerge, // sort merge join
    T_Sort,
    T_Projection,
    T_Agg,
    T_Having,
    T_Create_StaticCheckPoint,
    T_Crash,
    T_LoadData,
    T_IoEnable
} PlanTag;

// 查询执行计划
class Plan
{
public:
    PlanTag tag;

    virtual ~Plan() = default;
};

class ScanPlan : public Plan
{
public:
    ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, const std::vector<std::string>& index_col_names)
    {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        index_meta_ = sm_manager->db_.get_table(tab_name_)->get_index_meta(index_col_names);
    }

    ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, const IndexMeta& index_meta)
    {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        index_meta_ = index_meta;
    }

    ~ScanPlan() {}

    // 以下变量同ScanExecutor中的变量
    std::string tab_name_;
    std::vector<Condition> conds_;
    IndexMeta index_meta_;
};

class JoinPlan : public Plan
{
public:
    JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds)
    {
        Plan::tag = tag;
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        type = INNER_JOIN;
    }

    JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds, TabCol left_join_col_, TabCol right_join_col_, std::vector<std::string> tables_)
    {
        Plan::tag = tag;
        tables = std::move(tables_);
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        type = INNER_JOIN;
        left_join_col = std::move(left_join_col_);
        right_join_col = std::move(right_join_col_);
    }

    ~JoinPlan() {}

    // 左节点
    std::shared_ptr<Plan> left_;
    // 右节点
    std::shared_ptr<Plan> right_;
    // 连接条件
    std::vector<Condition> conds_;
    // future
    JoinType type;

    TabCol left_join_col, right_join_col;

    std::vector<std::string> tables;
};

class ProjectionPlan : public Plan
{
public:
    ProjectionPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols)
    {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        sel_cols_ = std::move(sel_cols);
    }

    ~ProjectionPlan() {}

    std::shared_ptr<Plan> subplan_;
    std::vector<TabCol> sel_cols_;
};

class SortPlan : public Plan
{
public:
    SortPlan(PlanTag tag, std::shared_ptr<Plan> subplan, TabCol sel_col, bool is_desc)
    {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        sel_col_ = std::move(sel_col);
        is_desc_ = is_desc;
    }

    ~SortPlan() {}

    std::shared_ptr<Plan> subplan_;
    TabCol sel_col_;
    bool is_desc_;
};

// dml语句，包括insert; delete; update; select语句　
class DMLPlan : public Plan
{
public:
    DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::string tab_name, std::vector<Value> values, std::vector<Condition> conds, std::vector<SetClause> set_clauses)
    {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        tab_name_ = std::move(tab_name);
        values_ = std::move(values);
        conds_ = std::move(conds);
        set_clauses_ = std::move(set_clauses);
    }

    ~DMLPlan() {}

    std::shared_ptr<Plan> subplan_;
    std::string tab_name_;
    std::vector<Value> values_;
    std::vector<Condition> conds_;
    std::vector<SetClause> set_clauses_;
};

// ddl语句, 包括create/drop table; create/drop index;
class DDLPlan : public Plan
{
public:
    DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols)
    {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        cols_ = std::move(cols);
        tab_col_names_ = std::move(col_names);
    }

    ~DDLPlan() {}

    std::string tab_name_;
    std::vector<std::string> tab_col_names_;
    std::vector<ColDef> cols_;
};

// help; show tables; desc tables; begin; abort; commit; rollback语句对应的plan
class OtherPlan : public Plan
{
public:
    OtherPlan(PlanTag tag, bool io_enabled_)
    {
        Plan::tag = tag;
        io_enable_ = io_enabled_;
    }
    OtherPlan(PlanTag tag, std::string tab_name)
    {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
    }

    OtherPlan(PlanTag tag, std::string tab_name, std::string file_name)
    {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        file_name_ = std::move(file_name);
    }

    ~OtherPlan() {}

    std::string tab_name_;
    std::string file_name_;
    bool io_enable_;
};

// Set Knob Plan
class SetKnobPlan : public Plan
{
public:
    SetKnobPlan(ast::SetKnobType knob_type, bool bool_value)
    {
        Plan::tag = T_SetKnob;
        set_knob_type_ = knob_type;
        bool_value_ = bool_value;
    }

    ast::SetKnobType set_knob_type_;
    bool bool_value_;
};

class plannerInfo
{
public:
    std::shared_ptr<ast::SelectStmt> parse;
    std::vector<Condition> where_conds;
    std::vector<TabCol> sel_cols;
    std::shared_ptr<Plan> plan;
    std::vector<std::shared_ptr<Plan>> table_scan_executors;
    std::vector<SetClause> set_clauses;

    plannerInfo(std::shared_ptr<ast::SelectStmt> parse_) : parse(std::move(parse_)) {}
};

class AggPlan : public Plan
{
public:
    std::vector<TabCol> sel_cols_;
    std::shared_ptr<Plan> subplan_;
    std::vector<TabCol> group_by_cols;

    AggPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> group_by_cols, std::vector<TabCol> sel_cols_) : sel_cols_(std::move(sel_cols_)), subplan_(std::move(subplan)), group_by_cols(std::move(group_by_cols)) { Plan::tag = tag; }

    ~AggPlan() override = default;
};

class HavingPlan : public Plan
{
public:
    std::shared_ptr<Plan> subplan_;
    std::vector<TabCol> sel_cols_;
    std::vector<HavingCond> having_conds_{};

    HavingPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols, std::vector<HavingCond> having_conds) : subplan_(std::move(subplan)), sel_cols_(std::move(sel_cols)), having_conds_(std::move(having_conds)) { Plan::tag = tag; }

    ~HavingPlan() override = default;
};
