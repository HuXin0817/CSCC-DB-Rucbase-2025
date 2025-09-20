#pragma once

#include <cassert>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "common/config_finals.h"
#include "common/context_finals.h"

static const char *print_separator_str = "+------------------";

static const int print_separator_str_len = strlen(print_separator_str);

static const char *print_record_str = "|                  ";

static const int print_record_str_len = strlen(print_record_str);

class RecordPrinter
{
    size_t num_cols;

public:
    RecordPrinter(size_t num_cols_) : num_cols(num_cols_) {}

    void print_separator(Context *context) const
    {
        for (size_t i = 0; i < num_cols; i++)
        {
            memcpy(context->data_send_ + *(context->offset_), print_separator_str, print_separator_str_len);
            *(context->offset_) = *(context->offset_) + print_separator_str_len;
        }
        std::string str = "+\n";
        memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
        *(context->offset_) = *(context->offset_) + str.length();
    }

    static void print_record(const std::vector<std::string> &rec_str, Context *context)
    {
        if (context->data_send_is_full())
        {
            return;
        }
        for (const auto &col : rec_str)
        {
            // 先复制列的前缀框架
            memcpy(context->data_send_ + *(context->offset_), print_record_str, print_record_str_len);
            
            // 计算可用的显示宽度（减去边框字符）
            size_t available_width = print_record_str_len - 1;
            
            // 如果字符串长度超过可用宽度，截断字符串
            std::string display_str = col;
            if (col.length() > available_width) {
                display_str = col.substr(0, available_width - 3) + "...";
            }
            
            // 右对齐显示字符串
            size_t start_pos = available_width - display_str.length();
            memcpy(context->data_send_ + *(context->offset_) + start_pos, display_str.c_str(), display_str.length());
            
            *(context->offset_) = *(context->offset_) + print_record_str_len;
        }
        std::string str = "|\n";
        memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
        *(context->offset_) = *(context->offset_) + str.length();
    }

    static void print_record_count(size_t num_rec, Context *context)
    {
        if (context->data_send_is_full())
        {
            return;
        }
        std::string str = "Total record(s): " + std::to_string(num_rec) + '\n';
        memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
        *(context->offset_) = *(context->offset_) + str.length();
    }
};
