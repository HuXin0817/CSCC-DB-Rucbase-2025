
#undef NDEBUG

#include <cassert>

#include "parser.h"

int main()
{
    std::vector<std::string> sqls = {"update t1 set id=id-1;"};
    for (auto &sql : sqls)
    {
        std::cout << sql << std::endl;
        YY_BUFFER_STATE buf = yy_scan_string(sql.c_str());
        assert(yyparse() == 0);
        if (ast::parse_tree != nullptr)
        {
            ast::TreePrinter::print(ast::parse_tree);
            yy_delete_buffer(buf);
            std::cout << std::endl;
        }
        else
        {
            std::cout << "exit/EOF" << std::endl;
        }
    }
    ast::parse_tree.reset();
    return 0;
}
