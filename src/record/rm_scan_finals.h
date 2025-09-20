#pragma once

#include <queue>

#include "rm_defs_finals.h"

class RmScan : public RecScan
{
    std::unordered_set<char *>::const_iterator it;
    std::unordered_set<char *>::const_iterator end;

public:
    explicit RmScan(RmFileHandle *file_handle)
    {
        it = file_handle->records_.begin();
        end = file_handle->records_.end();
    }

    void next() override
    {
        ++it;
    }

    [[nodiscard]] bool is_end() const override
    {
        return it == end;
    }

    [[nodiscard]] char *rid() const override
    {
        return *it;
    }
};
