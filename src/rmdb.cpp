#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>

#include "analyze/analyze.h"
#include "errors.h"
#include "optimizer/optimizer.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "recovery/log_recovery.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 256

static bool should_exit = false;

// 构建全局所需的管理器对象
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
auto sm_manager = std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
auto lock_manager = std::make_unique<LockManager>();
auto txn_manager = std::make_unique<TransactionManager>(sm_manager.get(), lock_manager.get());
auto planner = std::make_unique<Planner>(sm_manager.get());
auto optimizer = std::make_unique<Optimizer>(sm_manager.get(), planner.get());
auto log_manager = std::make_unique<LogManager>(disk_manager.get());
auto recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(), sm_manager.get(), log_manager.get(), txn_manager.get());
auto ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), planner.get(), recovery.get());
auto portal = std::make_unique<Portal>(sm_manager.get());
auto analyze = std::make_unique<Analyze>(sm_manager.get());
pthread_mutex_t *buffer_mutex;
pthread_mutex_t *sockfd_mutex;

static jmp_buf jmpbuf;

void sigint_handler(int signo)
{
    // std::cout << "The Server receive Crtl+C, will been closed\n";
    log_manager->flush_log_to_disk();
    should_exit = true;
    longjmp(jmpbuf, 1);
}

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void SetTransaction(txn_id_t &txn_id, Context *context)
{
    context->txn_ = txn_manager->get_transaction(txn_id);
    if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED || context->txn_->get_state() == TransactionState::ABORTED)
    {
        context->txn_ = txn_manager->begin(nullptr, context->log_mgr_);
        txn_id = context->txn_->get_transaction_id();
        context->txn_->set_txn_mode(false);
    }
}

void *client_handler(void *sock_fd)
{
    int fd = *((int *)sock_fd);
    pthread_mutex_unlock(sockfd_mutex);

    int i_recvBytes;
    // 接收客户端发送的请求
    char data_recv[BUFFER_LENGTH];
    // 需要返回给客户端的结果
    char *data_send = new char[BUFFER_LENGTH];
    // 需要返回给客户端的结果的长度
    int offset = 0;
    // 记录客户端当前正在执行的事务ID
    txn_id_t txn_id = INVALID_TXN_ID;

    std::string output = "establish client connection, sockfd: " + std::to_string(fd) + "\n";
    std::cout << output;

    while (true)
    {
        std::cout << "Waiting for request..." << std::endl;
        memset(data_recv, 0, BUFFER_LENGTH);

        i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);

        if (i_recvBytes == 0)
        {
            std::cout << "Maybe the client has closed" << std::endl;
            break;
        }
        if (i_recvBytes == -1)
        {
            std::cout << "Client read error!" << std::endl;
            break;
        }

        printf("i_recvBytes: %d \n ", i_recvBytes);

        if (strcmp(data_recv, "exit") == 0)
        {
            std::cout << "Client exit." << std::endl;
            break;
        }
        if (strcmp(data_recv, "crash") == 0)
        {
            std::cout << "Server crash" << std::endl;
            exit(1);
        }

        std::cout << "Read from client " << fd << ": " << data_recv << std::endl;

        memset(data_send, '\0', BUFFER_LENGTH);
        offset = 0;

        // 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
        auto *context = new Context(lock_manager.get(), log_manager.get(), nullptr, data_send, &offset);
        SetTransaction(txn_id, context);
        assert(context->txn_ != nullptr);

        // 用于判断是否已经调用了yy_delete_buffer来删除buf
        bool finish_analyze = false;
        pthread_mutex_lock(buffer_mutex);
        YY_BUFFER_STATE buf = yy_scan_string(data_recv);
        if (yyparse() == 0)
        {
            if (ast::parse_tree != nullptr)
            {
                try
                {
                    // analyze and rewrite
                    std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
                    yy_delete_buffer(buf);
                    finish_analyze = true;
                    pthread_mutex_unlock(buffer_mutex);
                    // 优化器
                    std::shared_ptr<Plan> plan = optimizer->plan_query(query, context);
                    // portal
                    std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context);
                    portal->run(portalStmt, ql_manager.get(), &txn_id, context);
                    portal->drop();
                }
                catch (TransactionAbortException &e)
                {
                    // 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
                    std::string str = "abort\n";
                    std::memcpy(data_send, str.c_str(), str.length());
                    data_send[str.length()] = '\0';
                    offset = str.length();

                    // 回滚事务
                    txn_manager->abort(context->txn_, log_manager.get());
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
                    // 遇到异常，需要打印failure到output.txt文件中，并发异常信息返回给客户端
                    // std::cerr << e.what() << std::endl;

                    std::memcpy(data_send, e.what(), e.get_msg_len());
                    data_send[e.get_msg_len()] = '\n';
                    data_send[e.get_msg_len() + 1] = '\0';
                    offset = e.get_msg_len() + 1;

                    // 将报错信息写入output.txt
                    if (sm_manager->io_enabled_)
                    {
                        std::fstream outfile;
                        outfile.open("output.txt", std::ios::out | std::ios::app);
                        outfile << "failure\n";
                        outfile.close();
                    }
                }
            }
        }
        else
        {
            std::string ParseError = "parse error";
            std::memcpy(data_send, ParseError.c_str(), ParseError.length());
            data_send[ParseError.length()] = '\n';
            data_send[ParseError.length() + 1] = '\0';
            offset = ParseError.length() + 1;

            // 将报错信息写入output.txt
            if (sm_manager->io_enabled_)
            {
                std::fstream outfile;
                outfile.open("output.txt", std::ios::out | std::ios::app);
                outfile << "failure\n";
                outfile.close();
            }
        }
        if (!finish_analyze)
        {
            yy_delete_buffer(buf);
            pthread_mutex_unlock(buffer_mutex);
        }
        // future TODO: 格式化 sql_handler.result, 传给客户端
        // send result with fixed format, use protobuf in the future
        if (write(fd, data_send, offset + 1) == -1)
        {
            break;
        }
        // 如果是单条语句，需要按照一个完整的事务来执行，所以执行完当前语句后，自动提交事务
        if (!context->txn_->get_txn_mode())
        {
            txn_manager->commit(context->txn_, context->log_mgr_);
        }
    }

    // Clear
    std::cout << "Terminating current client_connection..." << std::endl;
    close(fd);             // close a file descriptor.
    pthread_exit(nullptr); // terminate calling thread!
}

void start_server()
{
    // init mutex
    buffer_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    sockfd_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(buffer_mutex, nullptr);
    pthread_mutex_init(sockfd_mutex, nullptr);
    /*
     * 这里我们动态分配了两个互斥锁 buffer_mutex 和 sockfd_mutex，并用 pthread_mutex_init 初始化它们。互斥锁用于在线程之间同步对共享资源的访问。
     */
    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in
    {
    };

    // 初始化连接
    /*创建一个用于监听连接的套接字 sockfd_server，使用 socket 函数创建一个 TCP 套接字。*/
    sockfd_server = socket(AF_INET, SOCK_STREAM, 0); // ipv4,TCP
    assert(sockfd_server != -1);
    int val = 1;
    /*setsockopt 设置套接字选项，这里设置 SO_REUSEADDR 允许重用本地地址，以避免地址占用的问题。*/
    if (setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) == -1)
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    // before bind(), set the attr of structure sockaddr.
    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY); // bind-addr all
    s_addr_in.sin_port = htons(SOCK_PORT);         // bind-port 8765
    fd_temp = bind(sockfd_server, (struct sockaddr *)(&s_addr_in), sizeof(s_addr_in));
    if (fd_temp == -1)
    {
        perror("bind");
        exit(EXIT_FAILURE);
    }

    fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
    if (fd_temp == -1)
    {
        std::cout << "Listen error!" << std::endl;
        exit(1);
    }

    while (!should_exit)
    {
        std::cout << "Waiting for new connection..." << std::endl;
        pthread_t thread_id;
        struct sockaddr_in s_addr_client
        {
        };
        int client_length = sizeof(s_addr_client);

        if (setjmp(jmpbuf))
        {
            std::cout << "Break from Server Listen Loop\n";
            break;
        }

        // Block here. Until server accepts a new connection.
        pthread_mutex_lock(sockfd_mutex);
        int sockfd = accept(sockfd_server, (struct sockaddr *)(&s_addr_client), (socklen_t *)(&client_length));
        if (sockfd == -1)
        {
            std::cout << "Accept error!" << std::endl;
            continue; // ignore current socket ,continue while loop.
        }

        // 和客户端建立连接，并开启一个线程负责处理客户端请求
        if (pthread_create(&thread_id, nullptr, &client_handler, (void *)(&sockfd)) != 0)
        {
            std::cout << "Create thread fail!" << std::endl;
            break; // break while loop
        }
    }

    // Clear
    std::cout << " Try to close all client-connection.\n";
    int ret = shutdown(sockfd_server, SHUT_WR); // shut down the all or part of a full-duplex connection.
    if (ret == -1)
    {
        printf("%s\n", strerror(errno));
    }
    //    assert(ret != -1);
    sm_manager->close_db();
    std::cout << " DB has been closed.\n";
    std::cout << "Server shuts down." << std::endl;
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        // 需要指定数据库名称
        std::cerr << "Usage: " << argv[0] << " <database>" << std::endl;
        exit(1);
    }

    signal(SIGINT, sigint_handler);

    try
    {
        std::string db_name;

        db_name = argv[1];

        if (!sm_manager->is_dir(db_name))
        {
            // Database not found, create a new one
            sm_manager->create_db(db_name);
        }

        // Open database
        sm_manager->open_db(db_name);

        // recovery database
        recovery->recovery();

        // Database name is passed by args

        start_server();
    }
    catch (RMDBError &e)
    {
        std::cerr << e.what() << std::endl;
        exit(1);
    }
    return 0;
}
