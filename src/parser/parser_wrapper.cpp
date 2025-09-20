#include "yacc.tab.hpp"
#include "parser_defs.h"

extern int yylex(yy::parser::semantic_type *yylval, yy::parser::location_type *yylloc, void *yyscanner);
extern YY_BUFFER_STATE yy_scan_string(const char *yy_str, yyscan_t yyscanner);
extern void yy_delete_buffer(YY_BUFFER_STATE buffer, yyscan_t yyscanner);

// Thread-local scanner to support legacy no-arg APIs used by some call sites
static thread_local yyscan_t tls_scanner = nullptr;

int yyparse(yyscan_t yyscanner) {
    yy::parser parser(yyscanner);
    return parser.parse();
}

int yyparse() {
    if (!tls_scanner) {
        yylex_init(&tls_scanner);
    }
    yy::parser parser(tls_scanner);
    return parser.parse();
}

YY_BUFFER_STATE yy_scan_string(const char *yy_str) {
    if (!tls_scanner) {
        yylex_init(&tls_scanner);
    }
    return ::yy_scan_string(yy_str, tls_scanner);
}

void yy_delete_buffer(YY_BUFFER_STATE buffer) {
    if (buffer) {
        ::yy_delete_buffer(buffer, tls_scanner);
    }
}
