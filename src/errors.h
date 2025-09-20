#pragma once

#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

class RMDBError : public std::exception
{
public:
    RMDBError() : _msg("Error: ") {}

    explicit RMDBError(const std::string &msg) : _msg("Error: " + msg) {}

    const char *what() const noexcept override { return _msg.c_str(); }

    int get_msg_len() const { return _msg.length(); }

    std::string _msg;
};

class InternalError : public RMDBError
{
public:
    explicit InternalError(const std::string &msg) : RMDBError(msg) {}
};

// PF errors
class UnixError : public RMDBError
{
public:
    UnixError() : RMDBError(strerror(errno)) {}
};

class FileOpenError : public RMDBError
{
public:
    explicit FileOpenError(const std::string &path) : RMDBError("File is open: " + path) {}
};

class FileNotOpenError : public RMDBError
{
public:
    explicit FileNotOpenError(int fd) : RMDBError("Invalid file descriptor: " + std::to_string(fd)) {}
};

class FileNotClosedError : public RMDBError
{
public:
    FileNotClosedError(const std::string &filename) : RMDBError("File is opened: " + filename) {}
};

class FileExistsError : public RMDBError
{
public:
    explicit FileExistsError(const std::string &filename) : RMDBError("File already exists: " + filename) {}
};

class FileNotFoundError : public RMDBError
{
public:
    explicit FileNotFoundError(const std::string &filename) : RMDBError("File not found: " + filename) {}
};

// RM errors
class RecordNotFoundError : public RMDBError
{
public:
    RecordNotFoundError(int page_no, int slot_no) : RMDBError("Record not found: (" + std::to_string(page_no) + "," + std::to_string(slot_no) + ")") {}
};

class InvalidRecordSizeError : public RMDBError
{
public:
    explicit InvalidRecordSizeError(int record_size) : RMDBError("Invalid record size: " + std::to_string(record_size)) {}
};

class InvalidMetaDataError : public RMDBError
{
public:
    explicit InvalidMetaDataError(const std::string &db_name) : RMDBError("Invalid meta data from database " + db_name) {};
};

// IX errors
class InvalidColLengthError : public RMDBError
{
public:
    explicit InvalidColLengthError(int col_len) : RMDBError("Invalid column length: " + std::to_string(col_len)) {}
};

class IndexEntryNotFoundError : public RMDBError
{
public:
    IndexEntryNotFoundError() : RMDBError("Index entry not found") {}
};

// SM errors
class DatabaseNotFoundError : public RMDBError
{
public:
    explicit DatabaseNotFoundError(const std::string &db_name) : RMDBError("Database not found: " + db_name) {}
};

class DatabaseExistsError : public RMDBError
{
public:
    explicit DatabaseExistsError(const std::string &db_name) : RMDBError("Database already exists: " + db_name) {}
};

class TableNotFoundError : public RMDBError
{
public:
    explicit TableNotFoundError(const std::string &tab_name) : RMDBError("Table not found: " + tab_name) {}
};

class TableExistsError : public RMDBError
{
public:
    explicit TableExistsError(const std::string &tab_name) : RMDBError("Table already exists: " + tab_name) {}
};

class ColumnNotFoundError : public RMDBError
{
public:
    explicit ColumnNotFoundError(const std::string &col_name) : RMDBError("Column not found: " + col_name) {}
};

class IndexNotFoundError : public RMDBError
{
public:
    IndexNotFoundError(const std::string &tab_name, const std::vector<std::string> &col_names)
    {
        _msg += "Index not found: " + tab_name + ".(";
        for (size_t i = 0; i < col_names.size(); ++i)
        {
            if (i > 0)
                _msg += ", ";
            _msg += col_names[i];
        }
        _msg += ")";
    }
};

class IndexExistsError : public RMDBError
{
public:
    IndexExistsError(const std::string &tab_name, const std::vector<std::string> &col_names)
    {
        _msg += "Index already exists: " + tab_name + ".(";
        for (size_t i = 0; i < col_names.size(); ++i)
        {
            if (i > 0)
                _msg += ", ";
            _msg += col_names[i];
        }
        _msg += ")";
    }
};

// QL errors
class InvalidValueCountError : public RMDBError
{
public:
    InvalidValueCountError() : RMDBError("Invalid value count") {}
};

class StringOverflowError : public RMDBError
{
public:
    StringOverflowError() : RMDBError("String is too long") {}
};

class IncompatibleTypeError : public RMDBError
{
public:
    IncompatibleTypeError(const std::string &lhs, const std::string &rhs) : RMDBError("Incompatible type error: lhs " + lhs + ", rhs " + rhs) {}
};

class AmbiguousColumnError : public RMDBError
{
public:
    explicit AmbiguousColumnError(const std::string &col_name) : RMDBError("Ambiguous column: " + col_name) {}
};

class PageNotExistError : public RMDBError
{
public:
    PageNotExistError(const std::string &table_name, int page_no) : RMDBError("Page " + std::to_string(page_no) + " in table " + table_name + "not exits") {}
};

class NullptrError : public RMDBError
{
public:
    NullptrError() : RMDBError("ptr is null!") {}

    NullptrError(const std::string &table_name, int page_no) : RMDBError("table " + table_name + ": ptr is null!") {}
};

class IndexAlreadyExistsError : public RMDBError
{
public:
    explicit IndexAlreadyExistsError(const std::string &index_name) : RMDBError("index " + index_name + " is already exists.") {}
};

class OpenDatabaseError : public RMDBError
{
public:
    explicit OpenDatabaseError(const std::string &db_name, const std::string &reason) : RMDBError("open database " + db_name + "error: " + reason) {}
};

class DropTableError : public RMDBError
{
public:
    explicit DropTableError(const std::string &table, const std::string &reason) : RMDBError("Drop table " + table + "error: " + reason) {}
};

class DropIndexError : public RMDBError
{
public:
    explicit DropIndexError(const std::string &index_name, const std::string &reason) : RMDBError("Drop index " + index_name + "error: " + reason) {}
};

class InvalidAggError : public RMDBError
{
public:
    explicit InvalidAggError(const std::string &col, const std::string &reason) : RMDBError("Can not use " + col + " with " + reason) {}
};

class IndexEntryAlreadyExistError : public RMDBError
{
public:
    explicit IndexEntryAlreadyExistError() : RMDBError("Index entry already exists") {}
};
