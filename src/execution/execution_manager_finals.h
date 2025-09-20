#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "common/common_finals.h"
#include "common/context_finals.h"
#include "executor_abstract_finals.h"
#include "optimizer/plan_finals.h"
#include "optimizer/planner_finals.h"
#include "transaction/transaction_manager_finals.h"

class Planner;

class QlManager
{
private:
    SmManager *sm_manager_;
    TransactionManager *txn_mgr_;
    Planner *planner_;
    bool ban_fh_ = false;

public:
    QlManager(SmManager *sm_manager, TransactionManager *txn_mgr, Planner *planner) : sm_manager_(sm_manager), txn_mgr_(txn_mgr), planner_(planner) {}

    void run_mutli_query(const std::shared_ptr<Plan> &plan, Context *context);

    void run_cmd_utility(const std::shared_ptr<Plan> &plan, txn_id_t *txn_id, Context *context);

    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, const std::vector<TabCol> &sel_cols, Context *context);

    static void run_dml(std::unique_ptr<AbstractExecutor> exec);

    static std::unordered_set<Value> sub_select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, bool converse_to_float = false);
};
