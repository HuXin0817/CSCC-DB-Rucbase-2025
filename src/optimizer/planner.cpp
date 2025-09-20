#include "planner.h"

#include <memory>

#include "execution/execution_merge_join.h"
#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

bool Planner::get_index_cols(const std::string &tab_name, const std::vector<Condition> &curr_conds, std::vector<std::string> &index_col_names)
{
    // 清空索引列名
    index_col_names.clear();
    // 获取表格对象
    auto &tab_ = sm_manager_->db_.get_table(tab_name);
    // 如果表格没有索引，返回false
    if (tab_.indexes.empty())
    {
        return false;
    }

    // 用于存储条件列的集合
    std::unordered_set<std::string> conds_cols_;
    // 遍历当前条件
    for (const auto &cond : curr_conds)
    {
        // 如果条件是列与值比较，并且操作符不是不等于，并且列属于当前表格
        if (cond.is_rhs_val && cond.op != CompOp::OP_NE && cond.lhs_col.tab_name == tab_name)
        {
            // 将列名加入集合
            conds_cols_.insert(cond.lhs_col.col_name);
        }
    }

    // 初始化匹配到的索引号为-1，最大匹配列数为0
    size_t matched_index_number_ = -1;
    int max_match_col_count_ = 0;
    // 遍历表格的索引
    for (size_t idx_number_ = 0; idx_number_ < tab_.indexes.size(); idx_number_++)
    {
        int match_col_num = 0;
        // 遍历索引的列
        for (int i = 0; i < tab_.indexes[idx_number_].col_num; i++)
        {
            // 如果当前索引列在条件列集合中
            if (conds_cols_.count(tab_.indexes[idx_number_].cols[i].name))
            {
                match_col_num++;
            }
            else
            {
                // 否则跳出循环
                break;
            }
        }
        // 如果匹配列数大于最大匹配列数
        if (match_col_num > max_match_col_count_)
        {
            max_match_col_count_ = match_col_num;
            matched_index_number_ = idx_number_;
        }
    }

    // 判断是否匹配到索引
    auto ans = matched_index_number_ != -1;
    // 如果匹配到索引
    if (ans)
    {
        // 将索引列名加入结果中
        for (int i = 0; i < tab_.indexes[matched_index_number_].col_num; i++)
        {
            index_col_names.push_back(tab_.indexes[matched_index_number_].cols[i].name);
        }
    }
    return ans;
}

bool Planner::get_merge_join_index(const std::string &tab_name, const TabCol &col)
{
    std::vector<std::string> index_col_names;
    index_col_names.push_back(col.col_name);

    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    if (tab.is_index(index_col_names))
        return true;
    return false;
}

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
std::vector<Condition> pop_conds(std::vector<Condition> &conds, std::string tab_names)
{
    // auto has_tab = [&](const std::string &tab_name) {
    //     return std::find(tab_names.begin(), tab_names.end(), tab_name) != tab_names.end();
    // };
    std::vector<Condition> solved_conds;
    auto it = conds.begin();
    while (it != conds.end())
    {
        if ((tab_names == it->lhs_col.tab_name && it->is_rhs_val) || (it->lhs_col.tab_name == it->rhs_col.tab_name) || (tab_names == it->lhs_col.tab_name && it->is_subquery))
        {
            solved_conds.emplace_back(std::move(*it));
            it = conds.erase(it);
        }
        else
        {
            it++;
        }
    }
    return solved_conds;
}

int push_conds(Condition *cond, std::shared_ptr<Plan> plan)
{
    if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan))
    {
        if (x->tab_name_.compare(cond->lhs_col.tab_name) == 0)
        {
            return 1;
        }
        else if (x->tab_name_.compare(cond->rhs_col.tab_name) == 0)
        {
            return 2;
        }
        else
        {
            return 0;
        }
    }
    else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan))
    {
        int left_res = push_conds(cond, x->left_);
        // 条件已经下推到左子节点
        if (left_res == 3)
        {
            return 3;
        }
        int right_res = push_conds(cond, x->right_);
        // 条件已经下推到右子节点
        if (right_res == 3)
        {
            return 3;
        }
        // 左子节点或右子节点有一个没有匹配到条件的列
        if (left_res == 0 || right_res == 0)
        {
            return left_res + right_res;
        }
        // 左子节点匹配到条件的右边
        if (left_res == 2)
        {
            // 需要将左右两边的条件变换位置
            std::map<CompOp, CompOp> swap_op = {
                {OP_EQ, OP_EQ},
                {OP_NE, OP_NE},
                {OP_LT, OP_GT},
                {OP_GT, OP_LT},
                {OP_LE, OP_GE},
                {OP_GE, OP_LE},
            };
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_op.at(cond->op);
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    return false;
}

std::shared_ptr<Plan> pop_scan(int *scantbl, std::string table, std::vector<std::string> &joined_tables, std::vector<std::shared_ptr<Plan>> plans)
{
    for (size_t i = 0; i < plans.size(); i++)
    {
        auto x = std::dynamic_pointer_cast<ScanPlan>(plans[i]);
        if (x->tab_name_.compare(table) == 0)
        {
            scantbl[i] = 1;
            joined_tables.emplace_back(x->tab_name_);
            return plans[i];
        }
    }
    return nullptr;
}

std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context)
{
    // TODO 实现逻辑优化规则

    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plan = make_one_rel(query, context);

    // 其他物理优化
    plan = generate_agg_plan(query, std::move(plan));
    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan));

    return plan;
}

std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query, Context *context)
{
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    std::vector<std::string> tables = query->tables;

    // 处理where里面的不相关子查询
    for (auto &cond : query->conds)
    {
        if (!cond.is_subquery || cond.subQuery->stmt == nullptr)
            continue;
        cond.subQuery->plan = do_planner(cond.subQuery->query, context);
    }

    // Scan table , 生成表算子列表tab_nodes
    std::vector<std::shared_ptr<Plan>> table_scan_executors(tables.size());
    for (size_t i = 0; i < tables.size(); i++)
    {
        auto curr_conds = pop_conds(query->conds, tables[i]);
        // int index_no = get_indexNo(tables[i], curr_conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
        if (!index_exist)
        { // 该表没有索引
            index_col_names.clear();
            table_scan_executors[i] = std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, tables[i], curr_conds, index_col_names);
        }
        else
        { // 存在索引
            table_scan_executors[i] = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, tables[i], curr_conds, index_col_names);
        }
    }

    // 只有一个表，不需要join。
    if (tables.size() == 1)
    {
        return table_scan_executors[0];
    }
    // 获取where条件
    auto conds = std::move(query->conds);
    std::shared_ptr<Plan> table_join_executors;

    int *scantbl = new int[tables.size()];
    for (size_t i = 0; i < tables.size(); i++)
    {
        scantbl[i] = -1;
    }
    // 假设在ast中已经添加了jointree，这里需要修改的逻辑是，先处理jointree，然后再考虑剩下的部分
    if (!conds.empty())
    {
        // 有连接条件

        // 根据连接条件，生成第一层join
        std::vector<std::string> joined_tables(tables.size());
        auto it = conds.begin();
        while (it != conds.end())
        {
            std::shared_ptr<Plan> left, right;
            left = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            right = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
            std::vector<Condition> join_conds{*it};
            // 建立join
            //  判断使用哪种join方式
            if (enable_nestedloop_join && enable_sortmerge_join)
            {
                // 默认nested loop join
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            }
            else if (enable_nestedloop_join)
            {
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            }
            else if (enable_sortmerge_join)
            {
                auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
                TabCol left_col, right_col;

                // 1、左表有序

                left = generate_join_sort_plan(it->lhs_col.tab_name, conds, left_col, left);
                if (x->has_sort && x->order->cols->col_name == left_col.col_name)
                {
                    x->has_sort = false;
                }

                // 2、右表有序

                right = generate_join_sort_plan(it->rhs_col.tab_name, conds, right_col, right);
                if (x->has_sort && x->order->cols->col_name == right_col.col_name)
                {
                    x->has_sort = false;
                }
                if (!left_col.empty() && !right_col.empty())
                    table_join_executors = std::make_shared<JoinPlan>(T_SortMerge, std::move(left), std::move(right), join_conds, left_col, right_col, tables);
                else
                    table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            }
            else
            {
                // error
                throw RMDBError("No join executor selected!");
            }

            // table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            it = conds.erase(it);
            break;
        }
        // 根据连接条件，生成第2-n层join
        it = conds.begin();
        while (it != conds.end())
        {
            std::shared_ptr<Plan> left_need_to_join_executors = nullptr;
            std::shared_ptr<Plan> right_need_to_join_executors = nullptr;
            bool isneedreverse = false;
            if (std::find(joined_tables.begin(), joined_tables.end(), it->lhs_col.tab_name) == joined_tables.end())
            {
                left_need_to_join_executors = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            }
            if (std::find(joined_tables.begin(), joined_tables.end(), it->rhs_col.tab_name) == joined_tables.end())
            {
                right_need_to_join_executors = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
                isneedreverse = true;
            }

            if (left_need_to_join_executors != nullptr && right_need_to_join_executors != nullptr)
            {
                std::vector<Condition> join_conds{*it};
                std::shared_ptr<Plan> temp_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left_need_to_join_executors), std::move(right_need_to_join_executors), join_conds);
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(temp_join_executors), std::move(table_join_executors), std::vector<Condition>());
            }
            else if (left_need_to_join_executors != nullptr || right_need_to_join_executors != nullptr)
            {
                if (isneedreverse)
                {
                    std::map<CompOp, CompOp> swap_op = {
                        {OP_EQ, OP_EQ},
                        {OP_NE, OP_NE},
                        {OP_LT, OP_GT},
                        {OP_GT, OP_LT},
                        {OP_LE, OP_GE},
                        {OP_GE, OP_LE},
                    };
                    std::swap(it->lhs_col, it->rhs_col);
                    it->op = swap_op.at(it->op);
                    left_need_to_join_executors = std::move(right_need_to_join_executors);
                }
                std::vector<Condition> join_conds{*it};
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left_need_to_join_executors), std::move(table_join_executors), join_conds);
            }
            else
            {
                push_conds(std::move(&(*it)), table_join_executors);
            }
            it = conds.erase(it);
        }
    }
    else
    {
        table_join_executors = table_scan_executors[0];
        scantbl[0] = 1;
    }

    // 连接剩余表
    for (size_t i = 0; i < tables.size(); i++)
    {
        if (scantbl[i] == -1)
        {
            table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(table_scan_executors[i]), std::move(table_join_executors), std::vector<Condition>());
        }
    }

    return table_join_executors;
}

std::shared_ptr<Plan> Planner::generate_agg_plan(const std::shared_ptr<Query> &query, std::shared_ptr<Plan> plan)
{
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);

    if (!x->has_agg && x->group_by == nullptr)
    {
        return plan;
    }
    // 获取分组列
    std::vector<TabCol> group_by_cols;
    if (x->group_by)
    {
        for (const auto &group_by_col : x->group_by->cols)
        {
            group_by_cols.push_back({group_by_col->tab_name, group_by_col->col_name});
        }
    }

    // 获取聚合函数
    std::vector<TabCol> sel_cols;
    for (const auto &col : query->cols)
    {
        sel_cols.push_back({col.tab_name, col.col_name, col.alias, col.aggFuncType});
    }
    auto agg_sel_cols = sel_cols;
    for (const auto &cond : query->having_conds)
    {
        if (std::find_if(agg_sel_cols.begin(), agg_sel_cols.end(), [&](const TabCol &col)
                         { return col.col_name == cond.lhs_col.col_name && col.tab_name == cond.lhs_col.tab_name && col.aggFuncType == cond.lhs_col.aggFuncType; }) == agg_sel_cols.end())
        {
            agg_sel_cols.push_back({cond.lhs_col.tab_name, cond.lhs_col.col_name, cond.lhs_col.alias, cond.lhs_col.aggFuncType});
        }
    }

    // 生成聚合计划
    plan = std::make_shared<AggPlan>(T_Agg, std::move(plan), group_by_cols, agg_sel_cols);

    // 如果有 HAVING 子句，则生成 HAVING 计划
    if (x->group_by && !x->group_by->having_conds.empty())
    {
        plan = std::make_shared<HavingPlan>(T_Having, std::move(plan), std::move(sel_cols), query->having_conds);
    }

    return plan;
}

std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan)
{
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if (!x->has_sort)
    {
        return plan;
    }
    std::vector<std::string> tables = query->tables;
    std::vector<ColMeta> all_cols;
    for (auto &sel_tab_name : tables)
    {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
    TabCol sel_col;
    for (auto &col : all_cols)
    {
        if (col.name == x->order->cols->col_name)
            sel_col = {.tab_name = col.tab_name, .col_name = col.name};
    }
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), sel_col, x->order->orderby_dir == ast::OrderBy_DESC);
}

std::shared_ptr<Plan> Planner::generate_join_sort_plan(const std::string &table, std::vector<Condition> &conds, TabCol &col, std::shared_ptr<Plan> plan)
{
    // 如果是索引扫描，直接返回
    if (plan->tag == T_IndexScan)
        return plan;

    // 查找适合的条件列
    for (auto &cond : conds)
    {
        if (cond.is_rhs_val || cond.op != OP_EQ)
            continue;

        if (cond.lhs_col.tab_name == table)
        {
            col = cond.lhs_col;
            cond.join_cond = true;
            break;
        }
        else if (cond.join_cond || cond.rhs_col.tab_name == table)
        {
            col = cond.rhs_col;
            cond.join_cond = true;
            break;
        }
    }

    // 如果没有找到合适的条件列，直接返回原计划
    if (col.empty())
        return plan;

    // 检查是否存在合适的索引
    auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan);
    bool exist_index = get_merge_join_index(scan_plan->tab_name_, col);
    if (exist_index)
    {
        std::vector<std::string> index_col_names = {col.col_name};
        return std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, scan_plan->tab_name_, scan_plan->conds_, index_col_names);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // 否则，返回排序计划
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), col, ast::OrderBy_DESC);
}

/**
 * @brief 生成 select plan
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context)
{
    query = logical_optimization(std::move(query), context);

    // 物理优化
    auto sel_cols = query->cols;
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, std::move(plannerRoot), std::move(sel_cols));

    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plannerRoot;

    // 生成DDL语句和DML语句
    if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->parse))
    {
        // create table;
        std::vector<ColDef> col_defs;
        for (auto &field : x->fields)
        {
            if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef>(field))
            {
                ColDef col_def = {.name = sv_col_def->col_name, .type = interp_sv_type(sv_col_def->type_len->type), .len = sv_col_def->type_len->len};
                col_defs.push_back(col_def);
            }
            else
            {
                throw InternalError("Unexpected field type");
            }
        }
        plannerRoot = std::make_shared<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
    }
    else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->parse))
    {
        // drop table;
        plannerRoot = std::make_shared<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(), std::vector<ColDef>());
    }
    else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex>(query->parse))
    {
        // create index;
        plannerRoot = std::make_shared<DDLPlan>(T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    }
    else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->parse))
    {
        // drop index
        plannerRoot = std::make_shared<DDLPlan>(T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    }
    else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(query->parse))
    {
        // insert;
        plannerRoot = std::make_shared<DMLPlan>(T_Insert, std::shared_ptr<Plan>(), x->tab_name, query->values, std::vector<Condition>(), std::vector<SetClause>());
    }
    else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->parse))
    {
        // delete;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);

        if (index_exist == false)
        { // 该表没有索引
            index_col_names.clear();
            table_scan_executors = std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }
        else
        { // 存在索引
            table_scan_executors = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }

        plannerRoot = std::make_shared<DMLPlan>(T_Delete, table_scan_executors, x->tab_name, std::vector<Value>(), query->conds, std::vector<SetClause>());
    }
    else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->parse))
    {
        // update;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);

        if (index_exist == false)
        { // 该表没有索引
            index_col_names.clear();
            table_scan_executors = std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }
        else
        { // 存在索引
            table_scan_executors = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }
        plannerRoot = std::make_shared<DMLPlan>(T_Update, table_scan_executors, x->tab_name, std::vector<Value>(), query->conds, query->set_clauses);
    }
    else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse))
    {
        // select
        std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo>(x);
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), std::vector<Value>(), std::vector<Condition>(), std::vector<SetClause>());
    }
    else
    {
        throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}
