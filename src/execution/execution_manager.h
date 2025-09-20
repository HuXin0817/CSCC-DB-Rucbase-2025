#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "common/common.h"
#include "common/context.h"
#include "execution_defs.h"
#include "executor_abstract.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "record/rm.h"
#include "recovery/log_recovery.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

class Planner;

class QlManager
{
private:
    SmManager *sm_manager_;
    TransactionManager *txn_mgr_;
    Planner *planner_;
    RecoveryManager *recovery_mgr_;

public:
    QlManager(SmManager *sm_manager, TransactionManager *txn_mgr, Planner *planner, RecoveryManager *recovery_mgr) : sm_manager_(sm_manager), txn_mgr_(txn_mgr), planner_(planner), recovery_mgr_(recovery_mgr) {}

    QlManager() = default;

    void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);

    void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context);

    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols, Context *context);

    void run_dml(std::unique_ptr<AbstractExecutor> exec);

    static std::unordered_set<Value> sub_select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, bool converse_to_float = false);
};
