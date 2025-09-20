#pragma once
enum class TransactionState
{
    DEFAULT = false,  // 默认状态，事务刚创建时的状态
    COMMITTED = true, // 提交状态，事务已成功提交，所有更改已持久化
};

enum WriteType : int
{
    INSERT_TUPLE = 0, // 插入操作，向表中插入一条新的记录
    DELETE_TUPLE,     // 删除操作，从表中删除一条记录
    UPDATE_TUPLE,     // 更新操作，更新表中的一条记录
    UPDATE_TUPLE_ON_INDEX,
};

class WriteRecord
{
public:
    WriteRecord(WriteType wtype, int fd, char *rid) : wtype_(wtype), fd_(fd), old_rid_(rid) {}

    WriteRecord(WriteType wtype, int fd, char *rid, char *record) : wtype_(wtype), fd_(fd), old_rid_(rid), new_rid_(record) {}

    WriteType wtype_; // 写操作类型
    int fd_;
    char *old_rid_; // 记录ID
    char *new_rid_ = nullptr;
};
