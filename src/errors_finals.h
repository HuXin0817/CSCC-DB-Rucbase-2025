#pragma once

class RMDBError : public std::exception
{
};

class IndexEntryAlreadyExistError : public RMDBError
{
};

class TransactionAbortException : public RMDBError
{
};
