#pragma once

#include <atomic>
#include <cstdlib>
#include <queue>
#include <thread>

static constexpr int MAX_PTR_SIZE = 500;

class spin_mutex
{
private:
    std::atomic_flag flag = ATOMIC_FLAG_INIT;

public:
    void lock()
    {
        while (flag.test_and_set(std::memory_order_acquire))
        {
            std::this_thread::yield();
        }
    }

    void unlock()
    {
        flag.clear(std::memory_order_release);
    }
};

class PoolManager
{
public:
    char *allocate(int size)
    {
        {
            std::unique_lock lock(latch_[size]);
            if (!cache_[size].empty())
            {
                auto ptr = cache_[size].front();
                cache_[size].pop();
                return ptr;
            }
        }
        return (char *)malloc(size);
    }

    void deallocate(char *ptr, int size)
    {
        std::unique_lock lock(latch_[size]);
        cache_[size].push(ptr);
    }

private:
    spin_mutex latch_[MAX_PTR_SIZE];
    std::queue<char *> cache_[MAX_PTR_SIZE];
};
