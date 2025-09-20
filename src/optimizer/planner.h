#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "analyze/analyze.h"
#include "common/common.h"
#include "common/context.h"
#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "parser/parser.h"
#include "plan.h"
#include "record/rm.h"
#include "system/sm.h"

class Planner
{
private:
    SmManager *sm_manager_;

    bool enable_nestedloop_join = true;
    bool enable_sortmerge_join = false;

public:
    Planner(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query, Context *context);

    void set_enable_nestedloop_join(bool set_val) { enable_nestedloop_join = set_val; }

    void set_enable_sortmerge_join(bool set_val) { enable_sortmerge_join = set_val; }

private:
    std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);
    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

    std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query, Context *context = nullptr);

    std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);

    std::shared_ptr<Plan> generate_agg_plan(const std::shared_ptr<Query> &query, std::shared_ptr<Plan> plan);

    bool get_index_cols(const std::string &tab_name, const std::vector<Condition> &curr_conds, std::vector<std::string> &index_col_names);

    ColType interp_sv_type(ast::SvType sv_type)
    {
        std::map<ast::SvType, ColType> m = {{ast::SV_TYPE_INT, TYPE_INT}, {ast::SV_TYPE_FLOAT, TYPE_FLOAT}, {ast::SV_TYPE_STRING, TYPE_STRING}, {ast::SV_TYPE_DATETIME, TYPE_DATETIME}};
        return m.at(sv_type);
    }
    std::shared_ptr<Plan> generate_join_sort_plan(const std::string &table, std::vector<Condition> &conds, TabCol &col, std::shared_ptr<Plan> plan);
    bool get_merge_join_index(const std::string &tab_name, const TabCol &col);
};
