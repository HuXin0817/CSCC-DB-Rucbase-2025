#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <variant>

#include "errors_finals.h"

enum JoinType
{
    INNER_JOIN,
    LEFT_JOIN,
    RIGHT_JOIN,
    FULL_JOIN
};
namespace ast
{

    enum SvType
    {
        SV_TYPE_INT,
        SV_TYPE_FLOAT,
        SV_TYPE_STRING,
        SV_TYPE_BOOL,
        SV_TYPE_DATETIME
    };

    enum SvCompOp
    {
        SV_OP_EQ,
        SV_OP_NE,
        SV_OP_LT,
        SV_OP_GT,
        SV_OP_LE,
        SV_OP_GE,
        SV_OP_IN,
        SV_OP_NOT_IN
    };

    enum OrderByDir
    {
        OrderBy_DEFAULT,
        OrderBy_ASC,
        OrderBy_DESC
    };

    enum SetKnobType
    {
        EnableNestLoop,
        EnableSortMerge
    };

enum TreeNodeType
{
    HelpNode,
    ShowTablesNode,
    TxnBeginNode,
    TxnCommitNode,
    TxnAbortNode,
    TxnRollbackNode,
    TypeLenNode,
    FieldNode,
    ColDefNode,
    CreateTableNode,
    DropTableNode,
    DescTableNode,
    DescIndexNode,
    CreateIndexNode,
    DropIndexNode,
    IoEnableNode,
    ExprNode,
    ValueNode,
    IntLitNode,
    FloatLitNode,
    StringLitNode,
    BoolLitNode,
    ColNode,
    AggFuncNode,
    SetClauseNode,
    BinaryExprNode,
    SubQueryExprNode,
    OrderByNode,
    CreateStaticCheckpointNode,
    CrashStmtNode,
    HavingCauseNode,
    GroupByNode,
    InsertStmtNode,
    DeleteStmtNode,
    UpdateStmtNode,
    JoinExprNode,
    SelectStmtNode,
    SetStmtNode,
    LoadStmtNode,

    UNKNOWN
};

    // Base class for tree nodes
    struct TreeNode
    {
        TreeNodeType type;
        virtual ~TreeNode()
        {
        } // enable polymorphism
    };

    struct Help : public TreeNode
    {
        Help() { type = HelpNode; }
    };

    struct ShowTables : public TreeNode
    {
        ShowTables() { type = ShowTablesNode; }
    };

    struct TxnBegin : public TreeNode
    {
        TxnBegin() { type = TxnBeginNode; }
    };

    struct TxnCommit : public TreeNode
    {
        TxnCommit() { type = TxnCommitNode; }
    };

    struct TxnAbort : public TreeNode
    {
        TxnAbort() { type = TxnAbortNode; }
    };

    struct TxnRollback : public TreeNode
    {
        TxnRollback() { type = TxnRollbackNode; }
    };

    struct TypeLen : public TreeNode
    {
        SvType type;
        int len;

        TypeLen(SvType type_, int len_) : type(type_), len(len_) { TreeNode::type = TypeLenNode; };
    };

    struct Field : public TreeNode
    {
        Field() { type = FieldNode; }
    };

    struct ColDef : public Field
    {
        std::string col_name;
        std::shared_ptr<TypeLen> type_len;

        ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_) : col_name(std::move(col_name_)), type_len(std::move(type_len_)) { type = ColDefNode; }
    };

    struct CreateTable : public TreeNode
    {
        std::string tab_name;
        std::vector<std::shared_ptr<Field>> fields;

        CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_) : tab_name(std::move(tab_name_)), fields(std::move(fields_)) { type = CreateTableNode; }
    };

    struct DropTable : public TreeNode
    {
        std::string tab_name;

        DropTable(std::string tab_name_) : tab_name(std::move(tab_name_)) { type = DropTableNode; }
    };

    struct DescTable : public TreeNode
    {
        std::string tab_name;

        DescTable(std::string tab_name_) : tab_name(std::move(tab_name_)) { type = DescTableNode; }
    };

    struct DescIndex : public TreeNode
    {
        std::string tab_name;

        DescIndex(std::string tab_name_) : tab_name(std::move(tab_name_)) { type = DescIndexNode; }
    };

    struct CreateIndex : public TreeNode
    {
        std::string tab_name;
        std::vector<std::string> col_names;

        CreateIndex(std::string tab_name_, std::vector<std::string> col_names_) : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) { type = CreateIndexNode; }
    };

    struct DropIndex : public TreeNode
    {
        std::string tab_name;
        std::vector<std::string> col_names;

        DropIndex(std::string tab_name_, std::vector<std::string> col_names_) : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) { type = DropIndexNode; }
    };

    struct IoEnable : public TreeNode
    {
        bool set_io_enable;
        explicit IoEnable(bool set_io_enable_) : set_io_enable(set_io_enable_) { type = IoEnableNode; }
    };

    struct Expr : public TreeNode
    {
        Expr() { type = ExprNode; }
    };

    struct Value : public Expr
    {
        Value() { type = ValueNode; }
    };

    struct IntLit : public Value
    {
        int val;

        IntLit(int val_) : val(val_) { type = IntLitNode; }
    };

    struct FloatLit : public Value
    {
        float val;

        FloatLit(float val_) : val(val_) { type = FloatLitNode; }
    };

    struct StringLit : public Value
    {
        std::string val;

        StringLit(std::string val_) : val(std::move(val_)) { type = StringLitNode; }
    };

    struct BoolLit : public Value
    {
        bool val;

        BoolLit(bool val_) : val(val_) { type = BoolLitNode; }
    };

    struct Col : public Expr
    {
        std::string tab_name;
        std::string col_name;

        std::string alias; // 别名

        Col(std::string tab_name_, std::string col_name_, std::string alias_ = "") : tab_name(std::move(tab_name_)), col_name(std::move(col_name_)), alias(std::move(alias_)) { type = ColNode; }
    };

    enum AggFuncType
    {
        default_type = 0,
        MAX,
        MIN,
        COUNT,
        AVG,
        SUM
    };

    struct AggFunc : public Col
    {
        AggFuncType type = default_type;

        AggFunc(std::string tab_name_, std::string col_name_, AggFuncType type_) : Col(std::move(tab_name_), std::move(col_name_)), type(type_) { TreeNode::type = AggFuncNode; }
    };

    struct SetClause : public TreeNode
    {
        std::string col_name;

        int op; // 0: +, 1: -, 2: *, 3: /
        std::shared_ptr<Value> val;

        bool self_update = false;

        SetClause(std::string col_name_, std::shared_ptr<Value> val_) : col_name(std::move(col_name_)), val(std::move(val_)) { op = -1; type = SetClauseNode; }
        SetClause(std::string col_name_, std::shared_ptr<Value> val_, int op_) : col_name(std::move(col_name_)), op(op_), val(std::move(val_)) { self_update = true; type = SetClauseNode; }
    };
    struct SelectStmt;
    struct BinaryExpr : public TreeNode
    {
        std::shared_ptr<Col> lhs;
        SvCompOp op;
        std::shared_ptr<Expr> rhs;

        BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_) : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) { type = BinaryExprNode; }
    };

    struct SubQueryExpr : public BinaryExpr
    {
        std::shared_ptr<SelectStmt> subquery;
        std::vector<std::shared_ptr<Value>> vals;

        SubQueryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<TreeNode> rhs_) : BinaryExpr(std::move(lhs_), op_, nullptr)
        {
            // 确保 rhs_ 是 SelectStmt 类型
            auto selectStmt = std::dynamic_pointer_cast<SelectStmt>(rhs_);
            if (!selectStmt)
            {
                throw RMDBError();
            }
            subquery = selectStmt;
            TreeNode::type = SubQueryExprNode;
        }

        SubQueryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::vector<std::shared_ptr<Value>> rhs_) : BinaryExpr(std::move(lhs_), op_, nullptr), vals(std::move(rhs_)) { TreeNode::type = SubQueryExprNode; }
    };

    struct OrderBy : public TreeNode
    {
        std::shared_ptr<Col> cols;
        OrderByDir orderby_dir;

        OrderBy(std::shared_ptr<Col> cols_, OrderByDir orderby_dir_) : cols(std::move(cols_)), orderby_dir(orderby_dir_) { type = OrderByNode; }
    };

    struct CreateStaticCheckpoint : public TreeNode
    {
    public:
        CreateStaticCheckpoint()
        {
            type = CreateStaticCheckpointNode;
        }
    };

    struct CrashStmt : public TreeNode
    {
    public:
        CrashStmt()
        {
            type = CrashStmtNode;
        }
    };

    struct HavingCause : public TreeNode
    {
        std::shared_ptr<AggFunc> lhs;
        SvCompOp op;
        std::shared_ptr<Expr> rhs;

        HavingCause(std::shared_ptr<AggFunc> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_) : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) { type = HavingCauseNode; }
    };

    struct GroupBy : public TreeNode
    {
        std::vector<std::shared_ptr<Col>> cols;
        std::vector<std::shared_ptr<HavingCause>> having_conds;

        explicit GroupBy(std::vector<std::shared_ptr<Col>> col_) : cols(std::move(col_)) { type = GroupByNode; }
    };

    struct InsertStmt : public TreeNode
    {
        std::string tab_name;
        std::vector<std::shared_ptr<Value>> vals;

        InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_) : tab_name(std::move(tab_name_)), vals(std::move(vals_)) { type = InsertStmtNode; }
    };

    struct DeleteStmt : public TreeNode
    {
        std::string tab_name;
        std::vector<std::shared_ptr<BinaryExpr>> conds;

        DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_) : tab_name(std::move(tab_name_)), conds(std::move(conds_)) { type = DeleteStmtNode; }
    };

    struct UpdateStmt : public TreeNode
    {
        std::string tab_name;
        std::vector<std::shared_ptr<SetClause>> set_clauses;
        std::vector<std::shared_ptr<BinaryExpr>> conds;

        UpdateStmt(std::string tab_name_, std::vector<std::shared_ptr<SetClause>> set_clauses_, std::vector<std::shared_ptr<BinaryExpr>> conds_) : tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)), conds(std::move(conds_)) { type = UpdateStmtNode; }
    };

    struct JoinExpr : public TreeNode
    {
        std::string left;
        std::string right;
        std::vector<std::shared_ptr<BinaryExpr>> conds;
        JoinType type;

        JoinExpr(std::string left_, std::string right_, std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_) : left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)), type(type_) { TreeNode::type = JoinExprNode; }
    };

    struct SelectStmt : public TreeNode
    {
        std::vector<std::shared_ptr<Col>> cols;
        std::vector<std::string> tabs;
        std::vector<std::shared_ptr<BinaryExpr>> conds;
        std::vector<std::shared_ptr<JoinExpr>> jointree;

        bool has_agg;
        std::shared_ptr<GroupBy> group_by;

        bool has_sort;
        std::shared_ptr<OrderBy> order;

        SelectStmt(std::vector<std::shared_ptr<Col>> cols_, std::vector<std::string> tabs_, std::vector<std::shared_ptr<BinaryExpr>> conds_, std::shared_ptr<GroupBy> group_by_, std::shared_ptr<OrderBy> order_) : cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)), group_by(std::move(group_by_)), order(std::move(order_))
        {
            has_sort = (bool)order;
            has_agg = false;
            type = SelectStmtNode;
        }
    };

    // set enable_nestloop
    struct SetStmt : public TreeNode
    {
        SetKnobType set_knob_type_;
        bool bool_val_;

        SetStmt(SetKnobType type, bool bool_value) : set_knob_type_(type), bool_val_(bool_value) { TreeNode::type = SetStmtNode; }
    };

    struct LoadStmt : public TreeNode
    {
        std::string file_name;
        std::string tab_name;

        LoadStmt(std::string file_name_, std::string table_name_) : file_name(std::move(file_name_)), tab_name(std::move(table_name_)) { type = LoadStmtNode; }
    };

    extern thread_local std::shared_ptr<ast::TreeNode> parse_tree;

} // namespace ast
