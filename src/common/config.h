#pragma once

#include <chrono>
#include <cstdint>

#define BUFFER_LENGTH 8192

static constexpr int INVALID_FRAME_ID = -1; // invalid frame id
static constexpr int INVALID_FILE_ID = -1;
static constexpr int INVALID_PAGE_ID = -1;          // invalid page id
static constexpr int INVALID_TXN_ID = -1;           // invalid transaction id
static constexpr int PAGE_SIZE = 4096;              // size of a data page in byte  4KB
                                                    // static constexpr int BUFFER_POOL_SIZE = 32768;  // size of buffer pool 128MB
                                                    // static constexpr int BUFFER_POOL_SIZE = 65536;  // size of buffer pool 256MB
                                                    // static constexpr int BUFFER_POOL_SIZE = 262144;        // size of buffer pool 1GB
static constexpr int BUFFER_POOL_SIZE = 262144 * 2; // size of buffer pool 2GB
// static constexpr int BUFFER_POOL_SIZE = 262144 * 4;        // size of buffer pool 4GB
// static constexpr int BUFFER_POOL_SIZE = 262144 * 8;        // size of buffer pool 8GB
static constexpr int LOG_BUFFER_SIZE = (1024 * PAGE_SIZE); // size of a log buffer in byte

using frame_id_t = int32_t;   // frame id type, 帧页ID, 页在BufferPool中的存储单元称为帧,一帧对应一页
using page_id_t = int32_t;    // page id type , 页ID
using txn_id_t = int32_t;     // transaction id type
using slot_offset_t = size_t; // slot offset type
using oid_t = uint16_t;
using timestamp_t = int32_t; // timestamp type, used for transaction concurrency

// log file
static const std::string LOG_FILE_NAME = "db.log";

// replacer
static const std::string REPLACER_TYPE = "LRU";

static const std::string DB_META_NAME = "db.meta";

static const std::string SUCCESS_MESSAGE = "SUCCESS!\n";
