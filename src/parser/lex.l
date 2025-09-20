/* keywords are case insensitive */
%option caseless
/* we don't need yywrap() function */
%option noyywrap
/* we don't need yyunput() function */
%option nounput
/* we don't need input() function */
%option noinput
/* use reentrant scanner, but no bison bridge macros */
%option reentrant

%{
#include "ast.h"
#include "yacc.tab.hpp"
#include <iostream>
#include <cstring>
#include <cstdlib>

// Match Bison C++ (lalr1.cc) yylex signature
#undef YY_DECL
#define YY_DECL int yylex(yy::parser::semantic_type *yylval, yy::parser::location_type *yylloc, void *yyscanner)

// automatically update location using Bison C++ location API
#define YY_USER_ACTION yylloc->step(); yylloc->columns(yyleng);

%}

alpha [a-zA-Z]
digit [0-9]
white_space [ \t]+
new_line "\r"|"\n"|"\r\n"
sign "+"|"-"
identifier {alpha}(_|{alpha}|{digit})*
value_int {sign}?{digit}+
value_float {sign}?{digit}+\.?({digit}+)?
value_string '[^']*'
single_op ";"|"("|")"|","|"*"|"="|">"|"<"|"\."
value_path [\.|\/][^ \t]+\.csv

%x STATE_COMMENT

%%
    /* block comment */
"/*" { BEGIN(STATE_COMMENT); }
<STATE_COMMENT>"*/" { BEGIN(INITIAL); }
<STATE_COMMENT>[^*] { /* ignore the text of the comment */ }
<STATE_COMMENT>\* { /* ignore *'s that aren't part of */ }
    /* single line comment */
"--".* { /* ignore single line comment */ }
    /* white space and new line */
{white_space} { /* ignore white space */ }
{new_line} { yylloc->lines(1); /* ignore new line */ }
    /* keywords */
"+"     { return yy::parser::token::SIGN_ADD; }
"-"     { return yy::parser::token::SIGN_SUB; }

"SELECT" { return yy::parser::token::SELECT; }
"UPDATE" { return yy::parser::token::UPDATE; }
"INSERT" { return yy::parser::token::INSERT; }
"DELETE" { return yy::parser::token::DELETE; }

"FROM"   { return yy::parser::token::FROM; }
"WHERE"  { return yy::parser::token::WHERE; }
"SET"    { return yy::parser::token::SET; }
"INTO"   { return yy::parser::token::INTO; }
"VALUES" { return yy::parser::token::VALUES; }
"AND"    { return yy::parser::token::AND; }

"BEGIN"    { return yy::parser::token::TXN_BEGIN; }
"COMMIT"   { return yy::parser::token::TXN_COMMIT; }
"ROLLBACK" { return yy::parser::token::TXN_ROLLBACK; }
"ABORT"    { return yy::parser::token::TXN_ABORT; }

"ORDER" { return yy::parser::token::ORDER; }
"BY"    { return yy::parser::token::BY; }
"GROUP" { return yy::parser::token::GROUP; }
"COUNT" { return yy::parser::token::COUNT; }
"SUM"   { return yy::parser::token::SUM; }
"MIN"   { return yy::parser::token::MIN; }
"AS"    { return yy::parser::token::AS; }
"JOIN"  { return yy::parser::token::JOIN; }


"CREATE"   { return yy::parser::token::CREATE; }
"TABLE"    { return yy::parser::token::TABLE; }
"INDEX"    { return yy::parser::token::INDEX; }
"INT"      { return yy::parser::token::INT; }
"CHAR"     { return yy::parser::token::CHAR; }
"FLOAT"    { return yy::parser::token::FLOAT; }
"DATETIME" { return yy::parser::token::DATETIME; }


"DROP"   { return yy::parser::token::DROP; }
"SHOW"   { return yy::parser::token::SHOW; }
"TABLES" { return yy::parser::token::TABLES; }
"DESC"   { return yy::parser::token::DESC; }
"MAX"    { return yy::parser::token::MAX; }
"AVG"    { return yy::parser::token::AVG; }
"HAVING" { return yy::parser::token::HAVING; }
"ASC"    { return yy::parser::token::ASC; }


"HELP" { return yy::parser::token::HELP; }
"EXIT" { return yy::parser::token::EXIT; }
"STATIC_CHECKPOINT" { return yy::parser::token::STATIC_CHECKPOINT; }
"CRASH" { return yy::parser::token::CRASH; }
"LOAD" { return yy::parser::token::LOAD; }
"ENABLE_NESTLOOP" { return yy::parser::token::ENABLE_NESTLOOP; }
"ENABLE_SORTMERGE" { return yy::parser::token::ENABLE_SORTMERGE; }
"TRUE" { 
    yylval->build<bool>();
    yylval->as<bool>() = true;
    return yy::parser::token::VALUE_BOOL; 
}
"FALSE" {
    yylval->build<bool>();
    yylval->as<bool>() = false;
    return yy::parser::token::VALUE_BOOL;
}
    /* operators */
">=" { return yy::parser::token::GEQ; }
"<=" { return yy::parser::token::LEQ; }
"<>" { return yy::parser::token::NEQ; }
"!=" { return yy::parser::token::NEQ; }
"IN" { return yy::parser::token::IN; }
"NOT" { return yy::parser::token::NOT; }
"OUTPUT_FILE" {return yy::parser::token::OUTPUT_FILE;}
"ON" {return yy::parser::token::ON;}
"OFF" {return yy::parser::token::OFF;}

{single_op} { return yytext[0]; }
    /* id */
{identifier} {
    yylval->build<std::string>();
    yylval->as<std::string>() = yytext;
    return yy::parser::token::IDENTIFIER;
}
    /* literals */
{value_int} {
    yylval->build<int>();
    yylval->as<int>() = atoi(yytext);
    return yy::parser::token::VALUE_INT;
}
{value_float} {
    yylval->build<float>();
    yylval->as<float>() = static_cast<float>(atof(yytext));
    return yy::parser::token::VALUE_FLOAT;
}
{value_string} {
    yylval->build<std::string>();
    yylval->as<std::string>() = std::string(yytext + 1, strlen(yytext) - 2);
    return yy::parser::token::VALUE_STRING;
}
{value_path} {
    yylval->build<std::string>();
    yylval->as<std::string>() = yytext;
    return yy::parser::token::VALUE_PATH;
}
    /* EOF */
<<EOF>> { return yy::parser::token::T_EOF; }
    /* unexpected char */
. { std::cerr << "Lexer Error: unexpected character " << yytext[0] << std::endl; }
%%
