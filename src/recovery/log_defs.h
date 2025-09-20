#pragma once

#include <atomic>
#include <chrono>

#include "common/config.h"
#include "defs.h"
#include "storage/disk_manager.h"

static constexpr std::chrono::duration<int64_t> FLUSH_TIMEOUT = std::chrono::seconds(3);
