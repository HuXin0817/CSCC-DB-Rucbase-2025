#pragma once

#include <cerrno>
#include <cstring>
#include <string>

#include "common/common_finals.h"
#include "execution/execution_group_finals.h"
#include "execution/execution_scaler_group_finals.h"
#include "execution/execution_merge_join_finals.h"
#include "execution/execution_sort_finals.h"
#include "execution/executor_abstract_finals.h"
#include "execution/executor_delete_finals.h"
#include "execution/executor_index_scan_finals.h"
#include "execution/executor_insert_finals.h"
#include "execution/executor_nestedloop_join_finals.h"
#include "execution/executor_projection_finals.h"
#include "execution/executor_seq_scan_finals.h"
#include "execution/executor_update_finals.h"
#include "optimizer/plan_finals.h"

typedef enum portalTag {
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT = 1,
    PORTAL_DML_WITHOUT_SELECT = 2,
    PORTAL_MULTI_QUERY = 3,
    PORTAL_CMD_UTILITY = 4
} portalTag;

// Portal 类可能负责处理用户请求并协调系统中的不同模块
struct PortalStmt {
    portalTag tag;

    std::vector<TabCol> sel_cols;
    std::unique_ptr<AbstractExecutor> root;
    std::shared_ptr<Plan> plan;

    PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::unique_ptr<AbstractExecutor> root_,
               std::shared_ptr<Plan> plan_) : tag(tag_), sel_cols(std::move(sel_cols_)), root(std::move(root_)),
                                              plan(std::move(plan_)) {
    }
};

// Portal 类可能负责处理用户请求并协调系统中的不同模块
class Portal {
private:
    SmManager *sm_manager_;

public:
    explicit Portal(SmManager *sm_manager) : sm_manager_(sm_manager) {
    }

    // 将查询执行计划转换成对应的算子树
    std::shared_ptr<PortalStmt> start(const std::shared_ptr<Plan> &plan, Context *context) {
        switch (plan->tag) {
            // OtherPlan tags
            case T_Help:
            case T_ShowTable:
            case T_DescTable:
            case T_DescIndex:
            case T_Transaction_begin:
            case T_Transaction_commit:
            case T_Transaction_abort:
            case T_Transaction_rollback:
            case T_Create_StaticCheckPoint:
            case T_Crash:
            case T_LoadData:
            case T_IoEnable: {
                return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(),
                                                    std::unique_ptr<AbstractExecutor>(), plan);
            }

            // SetKnobPlan tag
            case T_SetKnob: {
                return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(),
                                                    std::unique_ptr<AbstractExecutor>(), plan);
            }

            // DDLPlan tags
            case T_CreateTable:
            case T_DropTable:
            case T_CreateIndex:
            case T_DropIndex: {
                return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(),
                                                    std::unique_ptr<AbstractExecutor>(), plan);
            }

            // DMLPlan tags
            case T_select: {
                auto x = std::static_pointer_cast<DMLPlan>(plan);
                std::shared_ptr<ProjectionPlan> p = std::static_pointer_cast<ProjectionPlan>(x->subplan_);
                std::unique_ptr<AbstractExecutor> root = convert_plan_executor(p, context);
                return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(p->sel_cols_), std::move(root), plan);
            }

            case T_Update: {
                auto x = std::static_pointer_cast<DMLPlan>(plan);
                std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
                std::vector<char *> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }
                std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(
                    sm_manager_, x->tab_name_, std::move(x->set_clauses_), std::move(rids), context);
                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root),
                                                    plan);
            }

            case T_Delete: {
                auto x = std::static_pointer_cast<DMLPlan>(plan);
                std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
                std::vector<char *> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }
                std::unique_ptr<AbstractExecutor> root = std::make_unique<DeleteExecutor>(
                    sm_manager_, x->tab_name_, rids, context);
                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root),
                                                    plan);
            }

            case T_Insert: {
                auto x = std::static_pointer_cast<DMLPlan>(plan);
                std::unique_ptr<AbstractExecutor> root = std::make_unique<InsertExecutor>(
                    sm_manager_, x->tab_name_, x->values_, context);
                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root),
                                                    plan);
            }

            default:
                throw RMDBError();
        }
    }

    // 遍历算子树并执行算子生成执行结果
    static void run(const std::shared_ptr<PortalStmt> &portal, QlManager *ql, txn_id_t *txn_id, Context *context) {
        switch (portal->tag) {
            case PORTAL_ONE_SELECT: {
                ql->select_from(std::move(portal->root), portal->sel_cols, context);
                break;
            }
            // 处理一个不涉及 SELECT 的数据操作语言（DML）语句，例如 INSERT、UPDATE 或 DELETE。
            case PORTAL_DML_WITHOUT_SELECT: {
                QlManager::run_dml(std::move(portal->root));
                break;
            }
            // 处理一个包含多个查询的操作。
            case PORTAL_MULTI_QUERY: {
                ql->run_mutli_query(portal->plan, context);
                break;
            }
            // 处理一个实用程序命令（utility command），包括 CREATE、DROP、ALTER 等表定义和管理操作。
            case PORTAL_CMD_UTILITY: {
                ql->run_cmd_utility(portal->plan, txn_id, context);
                break;
            }
            default: {
                throw RMDBError();
            }
        }
    }

    std::unique_ptr<AbstractExecutor> convert_plan_executor(const std::shared_ptr<Plan> &plan, Context *context) {
        switch (plan->tag) {
            case T_Projection: {
                auto x = std::static_pointer_cast<ProjectionPlan>(plan);
                return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_);
            }

            case T_SeqScan:
            case T_IndexScan: {
                auto x = std::static_pointer_cast<ScanPlan>(plan);
                // 条件里面的子查询
                for (auto &cond: x->conds_) {
                    if (!cond.is_subquery || cond.subQuery->stmt == nullptr)
                        continue;
                    // 如果条件左边是浮点数，右边是整数，需要转换
                    bool convert = false;
                    if (cond.lhs.type == TYPE_FLOAT && cond.subQuery->subquery_type == TYPE_INT)
                        convert = true;
                    cond.subQuery->result = QlManager::sub_select_from(
                        std::move(start(cond.subQuery->plan, context)->root), convert);
                    // 如果是标量子查询，结果集大小不为1，报错
                    if (cond.subQuery->is_scalar && cond.subQuery->result.size() != 1) {
                        throw RMDBError();
                    }
                }

                if (x->tag == T_SeqScan) {
                    return std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->conds_, context);
                } else {
                    return std::make_unique<IndexScanExecutor>(sm_manager_, x->tab_name_, x->conds_, x->index_meta_,
                                                               context);
                }
            }

            case T_NestLoop:
            case T_SortMerge: {
                auto x = std::static_pointer_cast<JoinPlan>(plan);
                std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
                std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
                std::unique_ptr<AbstractExecutor> join;
                if (x->tag == T_NestLoop)
                    join = std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right),
                                                                    std::move(x->conds_));
                else
                    join = std::make_unique<MergeJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_),
                                                               x->left_join_col, x->right_join_col, x->tables);
                return join;
            }

            case T_Sort: {
                auto x = std::static_pointer_cast<SortPlan>(plan);
                return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context), x->sel_col_);
            }

            case T_Agg: {
                auto x = std::static_pointer_cast<AggPlan>(plan);
                if (x->sel_cols_.size() == 999)
                    return std::make_unique<ScalerAggPlanExecutor>(convert_plan_executor(x->subplan_, context),
                                                                   x->sel_cols_[0], context);
                else
                    return std::make_unique<AggPlanExecutor>(convert_plan_executor(x->subplan_, context),
                                                             x->group_by_cols, x->sel_cols_, context);
            }

            case T_Having: {
                auto x = std::static_pointer_cast<HavingPlan>(plan);
                return std::make_unique<HavingPlanExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_,
                                                            x->having_conds_, context);
            }

            default:
                return nullptr;
        }
    }
};
