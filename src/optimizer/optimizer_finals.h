#pragma once

#include <map>

#include "common/context_finals.h"
#include "errors_finals.h"
#include "parser/parser.h"
#include "plan_finals.h"
#include "planner_finals.h"
#include "transaction/transaction_manager_finals.h"

class Optimizer
{
private:
    Planner *planner_;

public:
    explicit Optimizer(Planner *planner) : planner_(planner) {}

    std::shared_ptr<Plan> plan_query(const std::shared_ptr<Query>& query, Context *context)
    {
        switch (query->parse->type) {
            case ast::HelpNode:
                // help;
                return std::make_shared<OtherPlan>(T_Help, std::string());
            
            case ast::ShowTablesNode:
                // show tables;
                return std::make_shared<OtherPlan>(T_ShowTable, std::string());
            
            case ast::DescTableNode:
            {
                // desc table;
                auto x = std::static_pointer_cast<ast::DescTable>(query->parse);
                return std::make_shared<OtherPlan>(T_DescTable, x->tab_name);
            }
            
            case ast::DescIndexNode:
            {
                // show index;
                auto x = std::static_pointer_cast<ast::DescIndex>(query->parse);
                return std::make_shared<OtherPlan>(T_DescIndex, x->tab_name);
            }
            
            case ast::TxnBeginNode:
                // begin;
                return std::make_shared<OtherPlan>(T_Transaction_begin, std::string());
            
            case ast::TxnAbortNode:
                // abort;
                return std::make_shared<OtherPlan>(T_Transaction_abort, std::string());
            
            case ast::TxnCommitNode:
                // commit;
                return std::make_shared<OtherPlan>(T_Transaction_commit, std::string());
            
            case ast::TxnRollbackNode:
                // rollback;
                return std::make_shared<OtherPlan>(T_Transaction_rollback, std::string());
            
            case ast::SetStmtNode:
            {
                // Set Knob Plan
                auto x = std::static_pointer_cast<ast::SetStmt>(query->parse);
                return std::make_shared<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
            }
            
            case ast::CreateStaticCheckpointNode:
                return std::make_shared<OtherPlan>(T_Create_StaticCheckPoint, std::string());
            
            case ast::CrashStmtNode:
                return std::make_shared<OtherPlan>(T_Crash, std::string());
            
            case ast::LoadStmtNode:
            {
                auto x = std::static_pointer_cast<ast::LoadStmt>(query->parse);
                return std::make_shared<OtherPlan>(T_LoadData, x->tab_name, x->file_name);
            }
            
            case ast::IoEnableNode:
            {
                auto x = std::static_pointer_cast<ast::IoEnable>(query->parse);
                return std::make_shared<OtherPlan>(T_IoEnable, x->set_io_enable);
            }
            
            default:
                return planner_->do_planner(query, context);
        }
    }
};
