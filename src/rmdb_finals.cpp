#define NDEBUG

#include <netinet/in.h>
#include <sys/resource.h>
#include <unistd.h>

#include <atomic>
#include <csetjmp>
#include <cstdlib>
#include <iomanip>
#include <regex>

#include "analyze/analyze_finals.h"
#include "errors_finals.h"
#include "optimizer/optimizer_finals.h"
#include "optimizer/plan_finals.h"
#include "optimizer/planner_finals.h"
#include "portal_finals.h"
#include "storage/memory_pool_manager.h"
#include "parser/parser_defs.h"
#include "cahce/cache.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 256

static bool should_exit = false;

auto memory_pool_manager = std::make_unique<PoolManager>();
auto sm_manager = std::make_unique<SmManager>(memory_pool_manager.get());
auto lock_manager = std::make_unique<LockManager>(memory_pool_manager.get());
auto txn_manager = std::make_unique<TransactionManager>(sm_manager.get(), lock_manager.get());
auto planner = std::make_unique<Planner>(sm_manager.get());
auto optimizer = std::make_unique<Optimizer>(planner.get());
auto ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), planner.get());
auto portal = std::make_unique<Portal>(sm_manager.get());
auto analyze = std::make_unique<Analyze>(sm_manager.get());
auto cache = std::make_unique<DBCahce>(sm_manager.get());

// pthread_mutex_t *buffer_mutex;
pthread_mutex_t *sockfd_mutex;

static jmp_buf jmpbuf;

int Context::MAX_OFFSET_LENGTH = BUFFER_LENGTH >> 1;

void SetTransaction(txn_id_t *txn_id, Context *context)
{
    context->txn_ = txn_manager->get_transaction(*txn_id);
    if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED)
    {
        context->txn_ = txn_manager->begin(nullptr);
        *txn_id = context->txn_->txn_id_;
        context->txn_->set_txn_mode(false);
    }
}

bool run_sql_command(int &fd, int &txn_id, char *data_recv, char *data_send)
{
    if (strcmp(data_recv, "exit") == 0)
    {
        return false;
    }

    memset(data_send, '\0', BUFFER_LENGTH);
    int offset = 0;

    auto *context = new Context(lock_manager.get(), nullptr, data_send, &offset);
    SetTransaction(&txn_id, context);
    // Prepare parser resources outside try so we can clean up safely
    yyscan_t scanner = nullptr;
    YY_BUFFER_STATE buf = nullptr;
    bool parse_ok = true;

    try
    {
        // Fast path: try cache-based execution first (use SQL input, not data_send buffer)
        if (cache->has_cache(data_recv, context))
        {
            // Cache path handled the operation (e.g., INSERT). Skip parsing and go to send/commit.
        }
        else
        {
            yylex_init(&scanner);
            buf = yy_scan_string(data_recv, scanner);

            while (true)
            {
                int rc = yyparse(scanner);
                if (rc != 0)
                {
                    parse_ok = false;
                    break;
                }
                // Parser sets parse_tree to nullptr on T_EOF or EXIT
                if (ast::parse_tree == nullptr)
                {
                    break;
                }

                // analyze, optimize, and execute
                std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
                std::shared_ptr<Plan> plan = optimizer->plan_query(query, context);
                std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context);
                portal->run(portalStmt, ql_manager.get(), &txn_id, context);

                // reset for next statement
                ast::parse_tree = nullptr;
            }
        }
    }
    catch (TransactionAbortException &e)
    {
        std::string str = "abort\n";
        memcpy(data_send, str.c_str(), str.length());
        data_send[str.length()] = '\0';
        offset = str.length();

        txn_manager->abort(context->txn_);

        if (sm_manager->io_enabled_)
        {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << str;
            outfile.close();
        }
    }
    catch (RMDBError &e)
    {
    // Write failure to output file if enabled
        if (sm_manager->io_enabled_)
        {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << "failure\n";
            outfile.close();
        }
    // Also return a generic failure line to client buffer
    const char *msg = "failure";
    size_t len = strlen(msg);
    memcpy(data_send, msg, len);
    data_send[len] = '\n';
    data_send[len + 1] = '\0';
    offset = static_cast<int>(len + 1);
    }

    // cleanup scanner buffer after finishing all statements (if created)
    if (buf != nullptr && scanner != nullptr)
    {
        yy_delete_buffer(buf, scanner);
    }
    if (scanner != nullptr)
    {
        yylex_destroy(scanner);
    }

    if (!parse_ok)
    {
        // mark failure in output; also return a parse error to client
        const char *ParseError = "parse error";
        size_t len = strlen(ParseError);
        memcpy(data_send, ParseError, len);
        data_send[len] = '\n';
        data_send[len + 1] = '\0';
        offset = static_cast<int>(len + 1);

        if (sm_manager->io_enabled_)
        {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << "failure\n";
            outfile.close();
        }
    }

    if (write(fd, data_send, offset + 1) == -1)
    {
        return false;
    }
    if (!context->txn_->get_txn_mode())
    {
        txn_manager->commit(context->txn_);
    }
    delete context;
    return true;
}

void *client_handler(void *sock_fd)
{
    int fd = *((int *)sock_fd);
    pthread_mutex_unlock(sockfd_mutex);

    char data_recv[BUFFER_LENGTH];
    char data_send[BUFFER_LENGTH];
    txn_id_t txn_id = INVALID_TXN_ID;

    while (true)
    {
        memset(data_recv, 0, BUFFER_LENGTH);
        auto i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);
        if (i_recvBytes == 0)
        {
            return nullptr;
        }
        if (i_recvBytes == -1)
        {
            return nullptr;
        }
        if (!run_sql_command(fd, txn_id, data_recv, data_send))
        {
            return nullptr;
        }
    }
}

void start_server()
{
    // buffer_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    sockfd_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    // pthread_mutex_init(buffer_mutex, nullptr);
    pthread_mutex_init(sockfd_mutex, nullptr);

    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in;

    sockfd_server = socket(AF_INET, SOCK_STREAM, 0);
    assert(sockfd_server != -1);
    int val = 1;
    setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
    s_addr_in.sin_port = htons(SOCK_PORT);
    fd_temp = bind(sockfd_server, (struct sockaddr *)(&s_addr_in), sizeof(s_addr_in));
    if (fd_temp == -1)
    {
        exit(0);
    }

    fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
    if (fd_temp == -1)
    {
        exit(0);
    }

    while (!should_exit)
    {
        pthread_t thread_id;
        struct sockaddr_in s_addr_client
        {
        };
        int client_length = sizeof(s_addr_client);

        if (setjmp(jmpbuf))
        {
            break;
        }

        pthread_mutex_lock(sockfd_mutex);
        int sockfd = accept(sockfd_server, (struct sockaddr *)(&s_addr_client), (socklen_t *)(&client_length));
        if (sockfd == -1)
        {
            continue;
        }

        if (pthread_create(&thread_id, nullptr, &client_handler, (void *)(&sockfd)) != 0)
        {
            break;
        }
    }
}

int main(int argc, char **argv)
{
    std::string db_name = argv[1];
    if (!sm_manager->is_dir(db_name))
    {
        sm_manager->create_db(db_name);
    }
    sm_manager->open_db(db_name);

    start_server();
    return 0;
}
