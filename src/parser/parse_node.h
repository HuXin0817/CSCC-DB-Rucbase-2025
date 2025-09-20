#pragma once

#include <memory>
#include <string>
#include <vector>

namespace ast
{

    // Base class for tree nodes
    struct TreeNode
    {
        virtual ~TreeNode()
        {
        } // enable polymorphism
    };

    struct Help : public TreeNode
    {
    };

    struct ShowTables : public TreeNode
    {
    };

    struct TxnBegin : public TreeNode
    {
    };

    struct TxnCommit : public TreeNode
    {
    };

    struct TxnAbort : public TreeNode
    {
    };

    struct TxnRollback : public TreeNode
    {
    };

} // namespace ast
