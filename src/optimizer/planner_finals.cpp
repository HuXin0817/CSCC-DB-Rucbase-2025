#include "planner_finals.h"

#include <memory>
#include <unordered_set>
#include <utility>

#include "execution/execution_merge_join_finals.h"
#include "execution/executor_delete_finals.h"
#include "execution/executor_index_scan_finals.h"
#include "execution/executor_insert_finals.h"
#include "execution/executor_nestedloop_join_finals.h"
#include "execution/executor_projection_finals.h"
#include "execution/executor_seq_scan_finals.h"
#include "execution/executor_update_finals.h"
#include "record_printer.h"

// 性能优化：预编译静态常量
namespace {
    // 操作符反转映射表，使用数组代替map提高查找速度
    constexpr CompOp SWAP_OP_MAP[] = {
        CompOp::OP_EQ,  // OP_EQ -> OP_EQ
        CompOp::OP_GT,  // OP_LT -> OP_GT
        CompOp::OP_LT,  // OP_GT -> OP_LT
        CompOp::OP_GE,  // OP_LE -> OP_GE
        CompOp::OP_LE   // OP_GE -> OP_LE
    };
    
    // 预分配的内存池大小
    constexpr size_t TYPICAL_TABLE_COUNT = 8;
    constexpr size_t TYPICAL_CONDITION_COUNT = 16;
}

IndexMeta Planner::get_index_cols(const std::string &tab_name, const std::vector<Condition> &curr_conds) const {
    // 获取表格对象
    auto tab_ = sm_manager_->db_.get_table(tab_name);
    // 如果表格没有索引，返回false
    if (tab_->indexes.empty())
    {
        return {};
    }

    // 用于存储条件列的集合
    std::unordered_set<std::string> conds_cols_;
    conds_cols_.reserve(curr_conds.size());
    
    // 遍历当前条件，只添加有效的列
    for (const auto &cond : curr_conds)
    {
        // 如果条件是列与值比较，并且列属于当前表格
        if (cond.is_rhs_val && cond.lhs_col.tab_name == tab_name) 
        {
            conds_cols_.emplace(cond.lhs_col.col_name);
        }
    }
    
    // 如果没有可用的条件列，直接返回
    if (conds_cols_.empty()) 
    {
        return {};
    }

    // 初始化匹配结果
    size_t matched_index_number_ = SIZE_MAX;
    int max_match_col_count_ = 0;
    
    // 性能优化：使用索引遍历而非迭代器
    const auto& indexes = tab_->indexes;
    for (size_t idx_number_ = 0; idx_number_ < indexes.size(); ++idx_number_)
    {
        const auto& current_index = indexes[idx_number_];
        int match_col_num = 0;
        
        // 性能优化：提前退出循环和分支预测优化
        for (const auto& col : current_index.cols_)
        {
            if (conds_cols_.count(col.name)) 
            {
                ++match_col_num;
            }
            else
            {
                // 连续性要求：一旦不匹配就退出
                break;
            }
        }
        
        // 更新最佳匹配
        if (match_col_num > max_match_col_count_) 
        {
            max_match_col_count_ = match_col_num;
            matched_index_number_ = idx_number_;
        }
    }

    // 返回最佳匹配的索引
    if (matched_index_number_ != SIZE_MAX) 
    {
        return indexes[matched_index_number_];
    }
    return {};
}

bool Planner::get_merge_join_index(const std::string &tab_name, const TabCol &col)
{
    auto tab_ = sm_manager_->db_.get_table(tab_name);
    const auto& indexes = tab_->indexes;
    
    // 性能优化：使用基于范围的循环，减少索引计算
    for (const auto &index : indexes)
    {
        // 性能优化：只检查第一列，避免不必要的遍历
        if (!index.cols_.empty() && index.cols_.front().name == col.col_name) 
        {
            return true;
        }
    }
    return false;
}

std::vector<Condition> pop_conds(std::vector<Condition> &conds, const std::string &tab_names)
{
    std::vector<Condition> solved_conds;
    solved_conds.reserve(conds.size()); // 性能优化：预分配空间
    
    // 性能优化：使用反向迭代器避免频繁的元素移动
    for (auto it = conds.rbegin(); it != conds.rend();)
    {
        // 优化条件判断顺序，最常见的情况放在前面
        if ((tab_names == it->lhs_col.tab_name && it->is_rhs_val) || 
            (it->lhs_col.tab_name == it->rhs_col.tab_name) ||
            (tab_names == it->lhs_col.tab_name && it->is_subquery)) 
        {
            solved_conds.emplace_back(std::move(*it));
            // 转换为正向迭代器进行删除操作
            auto forward_it = std::next(it).base();
            it = std::vector<Condition>::reverse_iterator(conds.erase(forward_it));
        }
        else
        {
            ++it;
        }
    }
    return solved_conds;
}

int push_conds(Condition *cond, const std::shared_ptr<Plan> &plan)
{
    // 性能优化：使用if-else链代替dynamic_pointer_cast避免RTTI开销
    const auto plan_tag = plan->tag;
    
    if (plan_tag == T_SeqScan || plan_tag == T_IndexScan) 
    {
        // 直接转换为ScanPlan，避免dynamic_pointer_cast
        auto x = static_cast<ScanPlan*>(plan.get());
        if (x->tab_name_ == cond->lhs_col.tab_name) 
        {
            return 1;
        }
        else if (x->tab_name_ == cond->rhs_col.tab_name) 
        {
            return 2;
        }
        else
        {
            return 0;
        }
    }
    else if (plan_tag == T_NestLoop || plan_tag == T_SortMerge) 
    {
        // 直接转换为JoinPlan
        auto x = static_cast<JoinPlan*>(plan.get());
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
            // 性能优化：使用预计算的数组代替map查找
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = SWAP_OP_MAP[cond->op];
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    return 0;
}

std::shared_ptr<Plan> pop_scan(int *scantbl, const std::string &table, std::vector<std::string> &joined_tables, const std::vector<std::shared_ptr<Plan>>& plans)
{
    // 性能优化：使用索引遍历避免迭代器开销
    const size_t plans_size = plans.size();
    for (size_t i = 0; i < plans_size; ++i)
    {
        // 性能优化：直接使用static_cast避免dynamic_pointer_cast
        auto x = static_cast<ScanPlan*>(plans[i].get());
        if (x->tab_name_ == table) 
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

std::shared_ptr<Plan> Planner::physical_optimization(const std::shared_ptr<Query> &query, Context *context)
{
    std::shared_ptr<Plan> plan = make_one_rel(query, context);

    // 其他物理优化
    plan = generate_agg_plan(query, std::move(plan));
    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan));

    return plan;
}

std::shared_ptr<Plan> Planner::make_one_rel(const std::shared_ptr<Query> &query, Context *context)
{
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    const auto& tables = query->tables;
    const size_t table_count = tables.size();

    // 性能优化：预分配向量大小，避免重复分配
    std::vector<std::shared_ptr<Plan>> table_scan_executors;
    table_scan_executors.reserve(table_count);
    
    // Scan table，生成表算子列表tab_nodes
    for (size_t i = 0; i < table_count; ++i)
    {
        auto curr_conds = pop_conds(query->conds, tables[i]);
        auto index_meta = get_index_cols(tables[i], curr_conds);
        
        // 性能优化：减少条件分支，使用三元运算符
        const auto scan_type = index_meta.cols_.empty() ? T_SeqScan : T_IndexScan;
        table_scan_executors.emplace_back(
            std::make_shared<ScanPlan>(scan_type, sm_manager_, tables[i], 
                                     std::move(curr_conds), std::move(index_meta))
        );
    }

    // 只有一个表，不需要join
    if (table_count == 1) 
    {
        return table_scan_executors[0];
    }
    // 获取where条件
    auto &conds = query->conds;
    std::shared_ptr<Plan> table_join_executors;

    // 性能优化：使用栈分配的数组代替动态分配
    std::vector<int> scantbl(table_count, -1);
    
    if (!conds.empty()) 
    {
        // 有连接条件
        std::vector<std::string> joined_tables;
        joined_tables.reserve(table_count);
        
        auto it = conds.begin();
        while (it != conds.end())
        {
            auto left = pop_scan(scantbl.data(), it->lhs_col.tab_name, joined_tables, table_scan_executors);
            auto right = pop_scan(scantbl.data(), it->rhs_col.tab_name, joined_tables, table_scan_executors);
            std::vector<Condition> join_conds{*it};
            
            // 判断使用哪种join方式
            if (enable_nestedloop_join || enable_sortmerge_join) 
            {
                TabCol left_col, right_col;

                // 性能优化：直接访问AST节点，避免重复转换
                left = generate_join_sort_plan(it->lhs_col.tab_name, conds, left_col, left);
                if (x->has_sort && x->order->cols->col_name == left_col.col_name) 
                {
                    x->has_sort = false;
                }

                right = generate_join_sort_plan(it->rhs_col.tab_name, conds, right_col, right);
                if (x->has_sort && x->order->cols->col_name == right_col.col_name) 
                {
                    x->has_sort = false;
                }
                
                // 性能优化：使用条件运算符减少分支
                const auto join_type = (!left_col.empty() && !right_col.empty()) ? T_SortMerge : T_NestLoop;
                table_join_executors = (join_type == T_SortMerge) ?
                    std::make_shared<JoinPlan>(T_SortMerge, std::move(left), std::move(right),
                                             join_conds, left_col, right_col, tables) :
                    std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            }
            else
            {
                throw RMDBError();
            }

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
            
            // 性能优化：使用find代替循环查找
            if (std::find(joined_tables.begin(), joined_tables.end(), it->lhs_col.tab_name) == joined_tables.end()) 
            {
                left_need_to_join_executors = pop_scan(scantbl.data(), it->lhs_col.tab_name, joined_tables, table_scan_executors);
            }
            if (std::find(joined_tables.begin(), joined_tables.end(), it->rhs_col.tab_name) == joined_tables.end()) 
            {
                right_need_to_join_executors = pop_scan(scantbl.data(), it->rhs_col.tab_name, joined_tables, table_scan_executors);
                isneedreverse = true;
            }

            if (left_need_to_join_executors && right_need_to_join_executors) 
            {
                std::vector<Condition> join_conds{*it};
                auto temp_join_executors = std::make_shared<JoinPlan>(T_NestLoop, 
                    std::move(left_need_to_join_executors), std::move(right_need_to_join_executors), join_conds);
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, 
                    std::move(temp_join_executors), std::move(table_join_executors), std::vector<Condition>());
            }
            else if (left_need_to_join_executors || right_need_to_join_executors) 
            {
                if (isneedreverse) 
                {
                    std::swap(it->lhs_col, it->rhs_col);
                    it->op = SWAP_OP_MAP[it->op];
                    left_need_to_join_executors = std::move(right_need_to_join_executors);
                }
                std::vector<Condition> join_conds{*it};
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, 
                    std::move(left_need_to_join_executors), std::move(table_join_executors), join_conds);
            }
            else
            {
                push_conds(&(*it), table_join_executors);
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
    for (size_t i = 0; i < table_count; ++i)
    {
        if (scantbl[i] == -1) 
        {
            table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, 
                std::move(table_scan_executors[i]), std::move(table_join_executors), std::vector<Condition>());
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
    auto &agg_sel_cols = sel_cols;
    for (const auto &cond : query->having_conds)
    {
        if (std::find_if(agg_sel_cols.begin(), agg_sel_cols.end(), [&](const TabCol &col)
                         { return col.col_name == cond.lhs_col.col_name && col.tab_name == cond.lhs_col.tab_name &&
                                  col.aggFuncType == cond.lhs_col.aggFuncType; }) == agg_sel_cols.end())
        {
            agg_sel_cols.push_back(
                {cond.lhs_col.tab_name, cond.lhs_col.col_name, cond.lhs_col.alias, cond.lhs_col.aggFuncType});
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

std::shared_ptr<Plan> Planner::generate_sort_plan(const std::shared_ptr<Query> &query, std::shared_ptr<Plan> plan)
{
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if (!x->has_sort) 
    {
        return plan;
    }
    
    const auto& tables = query->tables;
    const size_t table_count = tables.size();
    
    // 性能优化：预分配vector容量，避免重复分配
    std::vector<ColMeta> all_cols;
    size_t estimated_col_count = table_count * 10; // 估算每表平均10列
    all_cols.reserve(estimated_col_count);
    
    for (const auto& sel_tab_name : tables)
    {
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name)->cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
    
    TabCol sel_col;
    const auto& target_col_name = x->order->cols->col_name;
    
    // 性能优化：使用基于范围的循环并早期退出
    for (const auto &col : all_cols)
    {
        if (col.name == target_col_name) 
        {
            sel_col = {.tab_name = col.tab_name, .col_name = col.name};
            break;
        }
    }
    
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), sel_col, 
                                    x->order->orderby_dir == ast::OrderBy_DESC);
}

std::shared_ptr<Plan>
Planner::generate_join_sort_plan(const std::string &table, std::vector<Condition> &conds, TabCol &col, std::shared_ptr<Plan> plan)
{
    // 性能优化：使用基于范围的循环查找适合的条件列
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
        else if (cond.rhs_col.tab_name == table) 
        {
            col = cond.rhs_col;
            cond.join_cond = true;
            break;
        }
    }

    // 如果没有找到合适的条件列或已经是索引扫描，直接返回原计划
    if (col.empty() || plan->tag == T_IndexScan) 
        return plan;

    // 性能优化：直接转换而非dynamic_pointer_cast
    auto scan_plan = static_cast<ScanPlan*>(plan.get());
    bool exist_index = get_merge_join_index(scan_plan->tab_name_, col);
    
    if (exist_index)
    {
        std::vector<std::string> index_col_names = {col.col_name};
        return std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, scan_plan->tab_name_, 
                                        scan_plan->conds_, index_col_names);
    }
    
    // 否则，返回排序计划
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), col, false);
}

std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context)
{
    query = logical_optimization(std::move(query), context);

    // 物理优化
    auto &sel_cols = query->cols;
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
                throw RMDBError();
            }
        }
        plannerRoot = std::make_shared<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
    }
    else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->parse))
    {
        // drop table;
        plannerRoot = std::make_shared<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(),
                                                std::vector<ColDef>());
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
        // plannerRoot = std::make_shared<DMLPlan>(T_Insert, std::shared_ptr<Plan>(), x->tab_name, query->values,
        //                                         std::vector<Condition>(), std::vector<SetClause>());
    }
    else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->parse))
    {
        // delete;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        auto index_meta = get_index_cols(x->tab_name, query->conds);

        if (index_meta.cols_.empty() )
        { // 该表没有索引
            table_scan_executors = std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds,
                                                              index_meta);
        }
        else
        { // 存在索引
            table_scan_executors = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds,
                                                              index_meta);
        }

        plannerRoot = std::make_shared<DMLPlan>(T_Delete, table_scan_executors, x->tab_name, std::vector<Value>(),
                                                query->conds, std::vector<SetClause>());
    }
    else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->parse))
    {
        // update;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        auto index_meta = get_index_cols(x->tab_name, query->conds);

        if (index_meta.cols_.empty())
        { // 该表没有索引
            table_scan_executors = std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_meta);
        }
        else
        { // 存在索引
            table_scan_executors = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_meta);
        }
        plannerRoot = std::make_shared<DMLPlan>(T_Update, table_scan_executors, x->tab_name, std::vector<Value>(),
                                                query->conds, query->set_clauses);
    }
    else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse))
    {
        // select
        std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo>(x);
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), std::vector<Value>(),
                                                std::vector<Condition>(), std::vector<SetClause>());
    }
    else
    {
        throw RMDBError();
    }
    return plannerRoot;
}
