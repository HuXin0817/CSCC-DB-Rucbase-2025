#pragma once

struct RmRecord
{
    char *data = nullptr; // 记录的数据
    int size = 0;         // 记录的大小

    // 默认构造函数
    RmRecord() = default;

    RmRecord(char *data_, int size_) : data(data_), size(size_) {}

    explicit RmRecord(int size_)
    {
        size = size_;
        data = new char[size_];
    }
};
