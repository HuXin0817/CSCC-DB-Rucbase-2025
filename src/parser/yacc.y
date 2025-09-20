%{
#include "ast.h"
#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace ast;
%}

// enable location in error handler
%locations
// C++ parser skeleton with variant semantic values
%skeleton "lalr1.cc"
%define api.value.type variant
/* %define api.value.automove true */
// enable verbose syntax error message
%define parse.error verbose
%param {void *yyscanner}

// Make AST and STL types available in generated header
%code requires {
  #include "ast.h"
  #include <memory>
  #include <string>
  #include <vector>
}

%code provides {
  namespace yy { class parser; }
  int yylex(yy::parser::semantic_type* yylval, yy::parser::location_type* yylloc, void* yyscanner);
}

%code {
  // C++ parser error handler
  void yy::parser::error(const location_type& loc, const std::string& msg) {
    std::cerr << "Parser Error at line " << loc.begin.line << " column " << loc.begin.column << ": " << msg << std::endl;
  }
}

// keywords
%token SHOW TABLES CREATE TABLE DROP DESC INSERT INTO VALUES DELETE FROM ASC ORDER BY
WHERE UPDATE SET SELECT INT CHAR FLOAT DATETIME INDEX AND JOIN EXIT HELP TXN_BEGIN TXN_COMMIT TXN_ABORT TXN_ROLLBACK ENABLE_NESTLOOP ENABLE_SORTMERGE STATIC_CHECKPOINT CRASH
MAX MIN AVG COUNT SUM GROUP HAVING AS IN NOT LOAD SIGN_ADD SIGN_SUB
// non-keywords
%token LEQ NEQ GEQ T_EOF
%token OUTPUT_FILE ON OFF

// type-specific tokens
%token <std::string> IDENTIFIER VALUE_STRING VALUE_PATH
%token <int> VALUE_INT
%token <float> VALUE_FLOAT
%token <bool> VALUE_BOOL

// specify types for non-terminal symbol
%type <std::shared_ptr<ast::TreeNode>> stmt dbStmt ddl dml txnStmt setStmt crashStmt io_stmt
%type <std::shared_ptr<ast::Field>> field
%type <std::vector<std::shared_ptr<ast::Field>>> fieldList
%type <std::shared_ptr<ast::TypeLen>> type
%type <ast::SvCompOp> op
%type <std::shared_ptr<ast::Expr>> expr
%type <std::shared_ptr<ast::Value>> value
%type <std::vector<std::shared_ptr<ast::Value>>> valueList
%type <std::string> tbName colName ALIAS fileName
%type <std::vector<std::string>> tableList colNameList
%type <std::shared_ptr<ast::Col>> col
%type <std::shared_ptr<ast::AggFunc>> aggFunc
%type <std::vector<std::shared_ptr<ast::Col>>> colList selector
%type <std::shared_ptr<ast::SetClause>> setClause
%type <std::vector<std::shared_ptr<ast::SetClause>>> setClauses
%type <std::shared_ptr<ast::BinaryExpr>> condition
%type <std::vector<std::shared_ptr<ast::BinaryExpr>>> whereClause optWhereClause
%type <std::vector<std::shared_ptr<ast::HavingCause>>> havingClause optHavingClause
%type <std::shared_ptr<ast::HavingCause>> havingCondition
%type <std::shared_ptr<ast::GroupBy>> optGroupByClause groupByClause
%type <std::shared_ptr<ast::OrderBy>>  order_clause opt_order_clause
%type <ast::OrderByDir> opt_asc_desc
%type <ast::SetKnobType> set_knob_type

%%
start:
        stmt ';'
    {
        parse_tree = $1;
        YYACCEPT;
    }
    |   HELP
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
    |   EXIT
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    |   T_EOF
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    |  io_stmt
    {
        parse_tree = $1;
        YYACCEPT;
    }
    ;

stmt:
        dbStmt
    |   ddl
    |   dml
    |   txnStmt
    |   setStmt
    |   crashStmt
    ;

crashStmt:
        CRASH
    {
        $$ = std::make_shared<CrashStmt>();
    }
    ;

txnStmt:
        TXN_BEGIN
    {
        $$ = std::make_shared<TxnBegin>();
    }
    |   TXN_COMMIT
    {
        $$ = std::make_shared<TxnCommit>();
    }
    |   TXN_ABORT
    {
        $$ = std::make_shared<TxnAbort>();
    }
    | TXN_ROLLBACK
    {
        $$ = std::make_shared<TxnRollback>();
    }
    ;

dbStmt:
        SHOW TABLES
    {
        $$ = std::make_shared<ShowTables>();
    }
    |   LOAD fileName INTO tbName
    {
         $$ = std::make_shared<LoadStmt>(std::move($2), std::move($4));
    }
    ;

setStmt:
        SET set_knob_type '=' VALUE_BOOL
    {
        $$ = std::make_shared<SetStmt>($2, $4);
    }
    ;
io_stmt:
        SET OUTPUT_FILE ON
    {
        $$ = std::make_shared<IoEnable>(true);
    }
    |   SET OUTPUT_FILE OFF
    {
        $$ = std::make_shared<IoEnable>(false);
    }
    ;

ddl:
        CREATE TABLE tbName '(' fieldList ')'
    {
        $$ = std::make_shared<CreateTable>(std::move($3), std::move($5));
    }
    |   DROP TABLE tbName
    {
        $$ = std::make_shared<DropTable>(std::move($3));
    }
    |   DESC tbName
    {
        $$ = std::make_shared<DescTable>(std::move($2));
    }
    |   CREATE INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<CreateIndex>(std::move($3), std::move($5));
    }
    |   DROP INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<DropIndex>(std::move($3), std::move($5));
    }
    |  SHOW INDEX FROM tbName
    {
    	$$ = std::make_shared<DescIndex>(std::move($4));
    }
    |   CREATE STATIC_CHECKPOINT
    {
        $$ = std::make_shared<CreateStaticCheckpoint>();
    }
    ;

dml:
        INSERT INTO tbName VALUES '(' valueList ')'
    {
        $$ = std::make_shared<InsertStmt>(std::move($3), std::move($6));
    }
    |   DELETE FROM tbName optWhereClause
    {
        $$ = std::make_shared<DeleteStmt>(std::move($3), std::move($4));
    }
    |   UPDATE tbName SET setClauses optWhereClause
    {
        $$ = std::make_shared<UpdateStmt>(std::move($2), std::move($4), std::move($5));
    }
    |   SELECT selector FROM tableList optWhereClause optGroupByClause opt_order_clause
    {
	$$ = std::make_shared<SelectStmt>(std::move($2), std::move($4), std::move($5), std::move($6), std::move($7));
    }
    ;

fieldList:
        field
    {
        $$.reserve(8); // DDL通常有更多字段，增加预分配大小
        $$.emplace_back(std::move($1));
    }
    |   fieldList ',' field
    {
        $$ = std::move($1);
        $$.emplace_back(std::move($3));
    }
    ;

colNameList:
        colName
    {
        $$.reserve(4); // 预分配常见大小
        $$.emplace_back(std::move($1));
    }
    | colNameList ',' colName
    {
        $$ = std::move($1);
        $$.emplace_back(std::move($3));
    }
    ;

field:
        colName type
    {
        $$ = std::make_shared<ColDef>(std::move($1), std::move($2));
    }
    ;

type:
        INT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_INT, sizeof(int));
    }
    |   CHAR '(' VALUE_INT ')'
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_STRING, $3);
    }
    |   FLOAT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
    }
    |   DATETIME
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_DATETIME, 19);
    }
    ;

valueList:
        value
    {
        $$.reserve(8); // 预分配常见大小，INSERT语句通常有多个值
        $$.emplace_back(std::move($1));
    }
    |   valueList ',' value
    {
        $$ = std::move($1);
        $$.emplace_back(std::move($3));
    }
    ;

value:
        VALUE_INT
    {
        $$ = std::make_shared<IntLit>(std::move($1));
    }
    |   VALUE_FLOAT
    {
        $$ = std::make_shared<FloatLit>(std::move($1));
    }
    |   VALUE_STRING
    {
        $$ = std::make_shared<StringLit>(std::move($1));
    }
    |   VALUE_BOOL
    {
        $$ = std::make_shared<BoolLit>(std::move($1));
    }
    ;

condition:
        col op expr
    {
        $$ = std::make_shared<BinaryExpr>(std::move($1), $2, std::move($3));
    }
    |   col op '(' dml ')'
    {
	$$ = std::make_shared<SubQueryExpr>(std::move($1), $2, std::move($4));
    }
    |   col op '(' valueList ')'
    {
	$$ = std::make_shared<SubQueryExpr>(std::move($1), $2, std::move($4));
    }
    ;

havingCondition:
    aggFunc op expr
    {
	$$ = std::make_shared<HavingCause>(std::move($1), $2, std::move($3));
    }
    ;

optWhereClause:
        /* epsilon */
    {
        $$ = std::vector<std::shared_ptr<ast::BinaryExpr>>{};
    }
    |   WHERE whereClause
    {
        $$ = std::move($2);
    }
    ;

whereClause:
        condition 
    {
        $$.reserve(4); // 预分配常见大小，WHERE子句通常有多个条件
        $$.emplace_back(std::move($1));
    }
    |   whereClause AND condition
    {
        $$ = std::move($1);
        $$.emplace_back(std::move($3));
    }
    ;

col:
        tbName '.' colName
    {
        $$ = std::make_shared<Col>(std::move($1), std::move($3));
    }
    |   colName
    {
        $$ = std::make_shared<Col>("", std::move($1));
    }
    |   aggFunc
    {
        $$ = std::move($1);
    }
    |   colName AS ALIAS
    {
	$$ = std::make_shared<Col>("", std::move($1), std::move($3));
    }
    |   aggFunc AS ALIAS
    {
	$$ = std::move($1);
	$$->alias = std::move($3);
    }
    ;
aggFunc:
        SUM '(' col ')'
    {
        auto c = std::move($3);
        $$ = std::make_shared<AggFunc>(std::move(c->tab_name), std::move(c->col_name), AggFuncType::SUM);
    }
    |   MIN '(' col ')'
    {
        auto c = std::move($3);
        $$ = std::make_shared<AggFunc>(std::move(c->tab_name), std::move(c->col_name), AggFuncType::MIN);
    }
    |   MAX '(' col ')'
    {
        auto c = std::move($3);
        $$ = std::make_shared<AggFunc>(std::move(c->tab_name), std::move(c->col_name), AggFuncType::MAX);
    }
    |   AVG '(' col ')'
     {
         auto c = std::move($3);
         $$ = std::make_shared<AggFunc>(std::move(c->tab_name), std::move(c->col_name), AggFuncType::AVG);
     }
    |   COUNT '(' col ')'
    {
        auto c = std::move($3);
        $$ = std::make_shared<AggFunc>(std::move(c->tab_name), std::move(c->col_name), AggFuncType::COUNT);
    }
    |   COUNT '(' '*' ')'
    {
        $$ = std::make_shared<AggFunc>("", "*", AggFuncType::COUNT);
    }
    ;


colList:
        col
    {
        $$.reserve(8); // 预分配常见大小，SELECT通常有多个列
        $$.emplace_back(std::move($1));
    }
    |   colList ',' col
    {
        $$ = std::move($1);
        $$.emplace_back(std::move($3));
    }
    ;

optGroupByClause:
    /* empty */
    {
        $$ = nullptr;
    }
    | groupByClause optHavingClause
    {
        $$ = std::move($1);
        $$->having_conds = std::move($2);
    }
    ;

groupByClause:
    GROUP BY colList
    {
        $$ = std::make_shared<GroupBy>(std::move($3));
    }
    ;
optHavingClause:
    /* empty */
    {
        $$ = std::vector<std::shared_ptr<HavingCause>> {};
    }
    | HAVING  havingClause
    {
        $$ = std::move($2);
    }
    ;

havingClause:
      havingCondition
    {
        $$.reserve(4); // 预分配常见大小
        $$.emplace_back(std::move($1));
    }
    | havingClause AND havingCondition
    {
        $$ = std::move($1);
        $$.emplace_back(std::move($3));
    }
    ;

op:
        '='
    {
        $$ = SV_OP_EQ;
    }
    |   '<'
    {
        $$ = SV_OP_LT;
    }
    |   '>'
    {
        $$ = SV_OP_GT;
    }
    |   NEQ
    {
        $$ = SV_OP_NE;
    }
    |   LEQ
    {
        $$ = SV_OP_LE;
    }
    |   GEQ
    {
        $$ = SV_OP_GE;
    }
    |   IN
    {
	$$ = SV_OP_IN;
    }
    |   NOT IN
    {
    	$$ = SV_OP_NOT_IN;
    }
    ;

expr:
        value
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   col
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    ;

setClauses:
        setClause
    {
        $$.reserve(4); // 预分配常见大小
        $$.emplace_back(std::move($1));
    }
    |   setClauses ',' setClause
    {
        $$ = std::move($1);
        $$.emplace_back(std::move($3));
    }
    ;

setClause:
        colName '=' value
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($3));
    }
    |   colName '=' colName  value
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($4), 4);
    }
    |   colName '=' colName SIGN_ADD value
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($5), 0);
    }
    |   colName '=' colName SIGN_SUB value
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($5), 1);
    }
    |   colName '=' colName '*' value
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($5), 2);
    }
    |   colName '=' colName '/' value
    {
        $$ = std::make_shared<SetClause>(std::move($1), std::move($5), 3);
    }
    ;

selector:
        '*'
    {
        $$ = {};
    }
    |   colList
    {
        $$ = std::move($1);
    }
    ;

tableList:
        tbName
    {
        $$.reserve(4); // 预分配常见大小，JOIN通常涉及2-4个表
        $$.emplace_back(std::move($1));
    }
    |   tableList ',' tbName
    {
        $$ = std::move($1);
        $$.emplace_back(std::move($3));
    }
    |   tableList JOIN tbName
    {
        $$ = std::move($1);
        $$.emplace_back(std::move($3));
    }
    ;

opt_order_clause:
    ORDER BY order_clause      
    { 
        $$ = std::move($3); 
    }
    |   /* epsilon */ 
    { 
        $$ = nullptr; 
    }
    ;

order_clause:
      col  opt_asc_desc 
    { 
        $$ = std::make_shared<OrderBy>(std::move($1), $2);
    }
    ;   

opt_asc_desc:
    ASC          { $$ = OrderBy_ASC;     }
    |  DESC      { $$ = OrderBy_DESC;    }
    |       { $$ = OrderBy_DEFAULT; }
    ;    

set_knob_type:
    ENABLE_NESTLOOP { $$ = EnableNestLoop; }
    |   ENABLE_SORTMERGE { $$ = EnableSortMerge; }
    ;

tbName: IDENTIFIER;

ALIAS: IDENTIFIER;

colName: IDENTIFIER;

fileName: VALUE_PATH;
%%
