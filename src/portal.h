#pragma once

#include <cerrno>
#include <cstring>
#include <string>

#include "common/common.h"
#include "execution/execution_group.h"
#include "execution/execution_merge_join.h"
#include "execution/execution_sort.h"
#include "execution/executor_abstract.h"
#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "optimizer/plan.h"

typedef enum portalTag
{
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT = 1,
    PORTAL_DML_WITHOUT_SELECT = 2,
    PORTAL_MULTI_QUERY = 3,
    PORTAL_CMD_UTILITY = 4
} portalTag;

// Portal 类可能负责处理用户请求并协调系统中的不同模块
struct PortalStmt
{
    portalTag tag;

    std::vector<TabCol> sel_cols;
    std::unique_ptr<AbstractExecutor> root;
    std::shared_ptr<Plan> plan;

    PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::unique_ptr<AbstractExecutor> root_, std::shared_ptr<Plan> plan_) : tag(tag_), sel_cols(std::move(sel_cols_)), root(std::move(root_)), plan(std::move(plan_)) {}
};

// Portal 类可能负责处理用户请求并协调系统中的不同模块
class Portal
{
private:
    SmManager *sm_manager_;

public:
    explicit Portal(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    ~Portal() = default;

    // 将查询执行计划转换成对应的算子树
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan, Context *context)
    {
        // 这里可以将select进行拆分，例如：一个select，带有return的select等
        if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan))
        {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan);
            // 操作包括设置或调整数据库系统的某些运行参数，如缓存大小、超时设置、并发限制等
        }
        else if (auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan))
        {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan);
        }
        else if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan))
        {
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan);
            // 下面会处理不同类型的 DML 操作（选择、更新、删除、插入）
        }
        else if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan))
        {
            switch (x->tag)
            {
            case T_select:
            {
                std::shared_ptr<ProjectionPlan> p = std::dynamic_pointer_cast<ProjectionPlan>(x->subplan_);
                std::unique_ptr<AbstractExecutor> root = convert_plan_executor(p, context);
                return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(p->sel_cols_), std::move(root), plan);
            }

            case T_Update:
            {
                std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
                std::vector<Rid> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple())
                {
                    rids.push_back(scan->rid());
                }
                std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(sm_manager_, x->tab_name_, x->set_clauses_, x->conds_, rids, context);
                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
            }
            case T_Delete:
            {
                std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
                std::vector<Rid> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple())
                {
                    rids.push_back(scan->rid());
                }

                std::unique_ptr<AbstractExecutor> root = std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, x->conds_, rids, context);

                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
            }

            case T_Insert:
            {
                std::unique_ptr<AbstractExecutor> root = std::make_unique<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);

                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
            }

            default:
                throw InternalError("Unexpected field type");
                break;
            }
        }
        else
        {
            throw InternalError("Unexpected field type");
        }
        return nullptr;
    }

    // 遍历算子树并执行算子生成执行结果
    void run(std::shared_ptr<PortalStmt> portal, QlManager *ql, txn_id_t *txn_id, Context *context)
    {
        switch (portal->tag)
        {
        case PORTAL_ONE_SELECT:
        {
            ql->select_from(std::move(portal->root), std::move(portal->sel_cols), context);
            break;
        }
            // 处理一个不涉及 SELECT 的数据操作语言（DML）语句，例如 INSERT、UPDATE 或 DELETE。
        case PORTAL_DML_WITHOUT_SELECT:
        {
            ql->run_dml(std::move(portal->root));
            break;
        }
            // 处理一个包含多个查询的操作。
        case PORTAL_MULTI_QUERY:
        {
            ql->run_mutli_query(portal->plan, context);
            break;
        }
            // 处理一个实用程序命令（utility command），包括 CREATE、DROP、ALTER 等表定义和管理操作。
        case PORTAL_CMD_UTILITY:
        {
            ql->run_cmd_utility(portal->plan, txn_id, context);
            break;
        }
        default:
        {
            throw InternalError("Unexpected field type");
        }
        }
    }

    // 清空资源
    void drop() {}

    std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan, Context *context)
    {
        if (auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan))
        {
            return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_);
        }
        else if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan))
        {
            // 条件里面的子查询
            for (auto &cond : x->conds_)
            {
                if (!cond.is_subquery || cond.subQuery->stmt == nullptr)
                    continue;
                // 如果条件左边是浮点数，右边是整数，需要转换
                bool convert = false;
                if (cond.lhs->type == TYPE_FLOAT && cond.subQuery->subquery_type == TYPE_INT)
                    convert = true;
                cond.subQuery->result = QlManager::sub_select_from(std::move(start(cond.subQuery->plan, context)->root), convert);
                // 如果是标量子查询，结果集大小不为1，报错
                if (cond.subQuery->is_scalar && cond.subQuery->result.size() != 1)
                {
                    throw RMDBError("Scalar subquery result size is not 1");
                }
            }

            if (x->tag == T_SeqScan)
            {
                return std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->conds_, context);
            }
            else
            {
                return std::make_unique<IndexScanExecutor>(sm_manager_, x->tab_name_, x->conds_, x->index_col_names_, context);
            }
        }
        else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan))
        {
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
            std::unique_ptr<AbstractExecutor> join;
            if (x->tag == T_NestLoop)
                join = std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_));
            else
                join = std::make_unique<MergeJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_), x->left_join_col, x->right_join_col, x->tables);
            return join;
        }
        else if (auto x = std::dynamic_pointer_cast<SortPlan>(plan))
        {
            return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context), x->sel_col_, x->is_desc_);
        }
        else if (auto x = std::dynamic_pointer_cast<AggPlan>(plan))
        {
            return std::make_unique<AggPlanExecutor>(convert_plan_executor(x->subplan_, context), x->group_by_cols, x->sel_cols_, context);
        }
        else if (auto x = std::dynamic_pointer_cast<HavingPlan>(plan))
        {
            return std::make_unique<HavingPlanExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_, x->having_conds_, context);
        }
        return nullptr;
    }
};
