#include "analyze_finals.h"

// 初始化静态向量，按照 ast::SvCompOp 枚举顺序映射到 CompOp
// ast::SvCompOp: SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE, SV_OP_IN, SV_OP_NOT_IN
std::vector<CompOp> Analyze::CompOpMap = {
    OP_EQ,  // SV_OP_EQ
    OP_EQ,  // SV_OP_NE (注意：原代码中没有处理NE，这里暂时映射为EQ)
    OP_LT,  // SV_OP_LT
    OP_GT,  // SV_OP_GT
    OP_LE,  // SV_OP_LE
    OP_GE,  // SV_OP_GE
    OP_EQ,  // SV_OP_IN (注意：原代码中没有处理IN，这里暂时映射为EQ)
    OP_EQ   // SV_OP_NOT_IN (注意：原代码中没有处理NOT_IN，这里暂时映射为EQ)
};

std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    const auto &db_ = sm_manager_->db_;

    std::shared_ptr<Query> query = std::make_shared<Query>();
    switch (parse->type)
    {
    case ast::SelectStmtNode:
    {
        auto x = std::static_pointer_cast<ast::SelectStmt>(parse);
        // 处理表名
        query->tables = std::move(x->tabs);
        const bool only_one_table = (query->tables.size() == 1);

        for (const auto &t : query->tables) {
            if (!db_.is_table(t)) throw RMDBError();
        }

        // target list
        query->cols.reserve(x->cols.size() ? x->cols.size() : 4);

        for (auto &sv_sel_col : x->cols) {
            TabCol sel_col{.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name, .alias = sv_sel_col->alias};
            if (only_one_table) sel_col.tab_name = query->tables[0];

            if (sv_sel_col->type == ast::AggFuncNode) {
                auto y = std::static_pointer_cast<ast::AggFunc>(sv_sel_col);
                sel_col.aggFuncType = y->type;
                x->has_agg = true;
            }
            query->cols.emplace_back(std::move(sel_col));
        }

        if (query->cols.empty()) {
            std::vector<ColMeta> all_cols;
            all_cols.reserve(16); // 先给个保守值，get_all_cols 内会追加
            get_all_cols(query->tables, all_cols);
            query->cols.reserve(all_cols.size());
            for (const auto &col : all_cols) {
                query->cols.push_back(TabCol{.tab_name = col.tab_name, .col_name = col.name});
            }
        } else {
            // 列元校验 & 补齐表名
            for (auto &sel_col : query->cols) {
                if (sel_col.col_name != "*") {
                    check_column(sel_col);
                }
            }
        }

        // GROUP BY / HAVING
        get_having(x->group_by, query->having_conds, only_one_table ? query->tables[0] : std::string{}, query->cols);

        // WHERE
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
        break;
    }
    case ast::UpdateStmtNode:
    {
        auto x = std::static_pointer_cast<ast::UpdateStmt>(parse);
        query->tables.emplace_back(x->tab_name);
        if (!db_.is_table(x->tab_name)) throw RMDBError();

        const auto tab = sm_manager_->db_.get_table(x->tab_name);
        query->set_clauses.reserve(x->set_clauses.size());

        // 处理需要更新的列和值
        for (const auto &set_clause : x->set_clauses)
        {
            // 使用局部变量来避免多次创建对象
            SetClause update_clause(convert_sv_value(set_clause->val));

            if (set_clause->self_update)
            {
                update_clause.set_op(set_clause->op);
            }
            else
            {
                update_clause.op = UpdateOp::ASSINGMENT;
            }

            // 类型转换

            auto &col = tab->get_col(set_clause->col_name);

            // 如果类型不匹配，进行类型转换
            if (col.type != update_clause.rhs.type)
            {
                if (!can_cast_type(update_clause.rhs.type, col.type))
                {
                    throw RMDBError();
                }
                else
                {
                    cast_value(update_clause.rhs, col.type);
                }
            }

            // 将更新的 SetClause 添加到 query 中
            update_clause.lhs = std::make_unique<ColMeta>(col);
            query->set_clauses.emplace_back(std::move(update_clause));
        }

        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
        break;
    }
    case ast::DeleteStmtNode:
    {
        auto x = std::static_pointer_cast<ast::DeleteStmt>(parse);
        // 处理表名
        query->tables.emplace_back(x->tab_name);

        // 检查表是否存在
        if (!sm_manager_->db_.is_table(x->tab_name))
        {
            throw RMDBError();
        }

        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
        break;
    }
    case ast::InsertStmtNode:
    {
        auto x = std::static_pointer_cast<ast::InsertStmt>(parse);
        // 处理表名
        query->tables.push_back(x->tab_name);

        // 检查表是否存在
        if (!sm_manager_->db_.is_table(x->tab_name))
        {
            throw RMDBError();
        }

        // 处理insert的values值
        for (auto &sv_val : x->vals)
        {
            //query->values.emplace_back(convert_sv_value(sv_val));
        }
        break;
    }
    default:
        // do nothing
        break;
    }
    query->parse = std::move(parse);
    return query;
}

void Analyze::check_column(TabCol &target)
{
    const auto &map = sm_manager_->col_meta_map_;
    if (target.tab_name.empty()) {
        auto it = map.find(target.col_name);
        if (it == map.end()) {
            throw RMDBError();
        }
        target.tab_name = it->second.tab_name;
    } else {
        auto it = map.find(target.col_name);
        if (it == map.end()) {
            throw RMDBError();
        }
    }
}


void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols)
{
    size_t total = 0;
    for (auto &t : tab_names) {
        total += sm_manager_->db_.get_table(t)->cols.size();
    }
    all_cols.reserve(all_cols.size() + total);

    for (auto &t : tab_names) {
        const auto &cols = sm_manager_->db_.get_table(t)->cols;
        all_cols.insert(all_cols.end(), cols.begin(), cols.end());
    }
}


void Analyze::get_having(std::shared_ptr<ast::GroupBy> &group_by, std::vector<HavingCond> &having_conds, const std::string &table_name, const std::vector<TabCol> &select_cols)
{
    bool has_non_agg_col = false;
    bool has_agg_func = false;

    // 检查 select_cols 中是否同时包含聚合函数和非聚合列
    for (const auto &sel_col : select_cols)
    {
        if (sel_col.aggFuncType != ast::default_type)
        {
            has_agg_func = true;
        }
        else
        {
            has_non_agg_col = true;
        }

        // 如果同时存在聚合函数和非聚合列，但没有 GROUP BY 子句，抛出异常
        if (has_non_agg_col && has_agg_func && !group_by)
        {
            throw RMDBError();
        }
    }

    if (!group_by)
    {
        return;
    }

    // 标记 group_by 中的列
    for (auto &col : group_by->cols)
    {
        col->tab_name = table_name;
    }
    // 检查 select_cols 的合法性
    for (const auto &select_col : select_cols)
    {
        bool is_valid = false;
        // 检查是否在 GROUP BY 列中
        for (const auto &group_col : group_by->cols)
        {
            if (select_col.col_name == group_col->col_name && select_col.tab_name == group_col->tab_name)
            {
                is_valid = true;
                break;
            }
        }
        // 检查是否是聚合函数
        if (!is_valid && select_col.aggFuncType != ast::default_type)
        {
            is_valid = true;
        }
        // 如果既不在 GROUP BY 列中，也不是聚合函数，则抛出异常
        if (!is_valid)
        {
            throw RMDBError();
        }
    }

    // 处理 having 条件
    auto sv_having_conds = group_by->having_conds;
    for (auto &expr : sv_having_conds)
    {
        HavingCond cond;
        cond.lhs_col = {.tab_name = table_name, .col_name = expr->lhs->col_name, .alias = expr->lhs->alias, .aggFuncType = expr->lhs->type};
        if (cond.lhs_col.col_name != "*")
            check_column(cond.lhs_col);
        cond.op = CompOpMap[expr->op];
        switch (expr->rhs->type)
        {
        case ast::ValueNode:
        case ast::IntLitNode:
        case ast::FloatLitNode:
        case ast::StringLitNode:
        case ast::BoolLitNode:
        {
            auto rhs_val = std::static_pointer_cast<ast::Value>(expr->rhs);
            cond.rhs_val = convert_sv_value(rhs_val);
            break;
        }
        default:
            throw RMDBError();
        }
        having_conds.push_back(cond);
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds,
                         std::vector<Condition> &conds)
{
    conds.clear();
    conds.reserve(sv_conds.size());

    for (auto &expr : sv_conds) {
        Condition cond;

        if (expr->lhs->type == ast::AggFuncNode) {
            throw RMDBError();
        }

        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        cond.op = CompOpMap[static_cast<size_t>(expr->op)];

        switch (expr->rhs->type)
        {
        case ast::ValueNode:
        case ast::IntLitNode:
        case ast::FloatLitNode:
        case ast::StringLitNode:
        case ast::BoolLitNode:
        {
            cond.is_rhs_val = true;
            auto rhs_val = std::static_pointer_cast<ast::Value>(expr->rhs);
            cond.rhs_val = convert_sv_value(rhs_val);
            break;
        }
        case ast::ColNode:
        {
            cond.is_rhs_val = false;
            auto rhs_col = std::static_pointer_cast<ast::Col>(expr->rhs);
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
            break;
        }
        default:
            throw RMDBError();
        }

        conds.emplace_back(std::move(cond));
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds)
{
    for (auto &cond : conds)
    {
        // Infer table name from column name
        if (tab_names.size()==1) {
            cond.lhs_col.tab_name = tab_names[0];
        }
        check_column(cond.lhs_col);
        if (!cond.is_rhs_val && !cond.is_subquery)
        {
            check_column(cond.rhs_col);
        }

        // Get lhs column metadata
        auto lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab->get_col(cond.lhs_col.col_name);
        cond.lhs = lhs_col;

        ColType lhs_type = lhs_col.type;
        ColType rhs_type;

        if (cond.is_rhs_val && !cond.is_subquery)
        {
            cond.rhs_val.init_raw(lhs_col.len);
            rhs_type = cond.rhs_val.type;

            // Check if rhs_val can be cast to lhs_type
            if (!can_cast_type(rhs_type, lhs_type))
            {
                throw RMDBError();
            }

            // Perform the cast if necessary
            if (rhs_type != lhs_type)
            {
                cast_value(cond.rhs_val, lhs_type);
            }
        }
        else if (!cond.is_subquery)
        {
            // Get rhs column metadata
            auto rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab->get_col(cond.rhs_col.col_name);
            cond.rhs = rhs_col;
            rhs_type = rhs_col.type;

            if (lhs_type != rhs_type)
            {
                throw RMDBError();
            }
        }
        // 遍历每一个cond
    }
    return;
}

bool Analyze::can_cast_type(ColType from, ColType to)
{
    // Add logic to determine if a type can be cast to another type
    if (from == to)
        return true;
    if (from == TYPE_INT && to == TYPE_FLOAT)
        return true;
    if (from == TYPE_FLOAT && to == TYPE_INT)
        return true;
    return false;
}

void Analyze::cast_value(Value &val, ColType to)
{
    // Add logic to cast val to the target type
    if (val.type == TYPE_INT && to == TYPE_FLOAT)
    {
        int int_val = val.int_val;
        val.type = TYPE_FLOAT;
        val.float_val = static_cast<float>(int_val);
    }
    else if (val.type == TYPE_FLOAT && to == TYPE_INT)
    {
        // do not things
    }
    else
    {
        throw RMDBError();
    }
}

Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val)
{
    Value val;
    switch (sv_val->type)
    {
    case ast::IntLitNode:
    {
        auto int_lit = std::static_pointer_cast<ast::IntLit>(sv_val);
        val.set_int(int_lit->val);
        break;
    }
    case ast::FloatLitNode:
    {
        auto float_lit = std::static_pointer_cast<ast::FloatLit>(sv_val);
        val.set_float(float_lit->val);
        break;
    }
    case ast::StringLitNode:
    {
        auto str_lit = std::static_pointer_cast<ast::StringLit>(sv_val);
        val.set_str(str_lit->val);
        break;
    }
    default:
        throw RMDBError();
    }
    return val;
}
