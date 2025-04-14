//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/enums/pending_execution_result.hpp"
#include "duckdb/common/enums/prepared_statement_mode.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"
#include "duckdb/transaction/transaction_context.hpp"

namespace duckdb {
class Appender;
class Catalog;
class CatalogSearchPath;
class ColumnDataCollection;
class DatabaseInstance;
class FileOpener;
class LogicalOperator;
class PreparedStatementData;
class Relation;
class BufferedFileWriter;
class QueryProfiler;
class ClientContextLock;
struct CreateScalarFunctionInfo;
class ScalarFunctionCatalogEntry;
struct ActiveQueryContext;
struct ParserOptions;
class SimpleBufferedData;
class BufferedData;
struct ClientData;
class ClientContextState;
class RegisteredStateManager;

struct PendingQueryParameters {
	//! Prepared statement parameters (if any)
	optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters;
	//! Whether a stream result should be allowed
	bool allow_stream_result = false;
};

/**
 * @class ClientContext
 * @brief 客户端会话上下文，管理单个客户端连接的所有执行状态
 *
 * 继承自 enable_shared_from_this 以支持共享指针管理，包含：
 * - 数据库连接信息
 * - 事务状态
 * - 查询执行控制
 * - 配置和日志
 */
class ClientContext : public enable_shared_from_this<ClientContext> {
    // 友元声明（允许特定类访问私有成员）
    friend class PendingQueryResult;  // 用于锁定上下文
    friend class BufferedData;        // 任务执行相关
    /* 其他友元类... */

public:
    //! @name 构造/析构
    //! @{
    DUCKDB_API explicit ClientContext(shared_ptr<DatabaseInstance> db);
    DUCKDB_API ~ClientContext();
    //! @}

    //! @name 核心成员
    //! @{
    shared_ptr<DatabaseInstance> db;          //!< 连接的数据库实例
    atomic<bool> interrupted;                 //!< 查询中断标志
    unique_ptr<RegisteredStateManager> registered_state; //!< 客户端状态管理器
    unique_ptr<Logger> logger;                 //!< 客户端专用日志器
    ClientConfig config;                      //!< 客户端配置
    unique_ptr<ClientData> client_data;        //!< 客户端运行时数据
    TransactionContext transaction;           //!< 事务管理上下文
    //! @}

    //! @name 事务管理
    //! @{
    MetaTransaction &ActiveTransaction() { return transaction.ActiveTransaction(); }
    DUCKDB_API void CancelTransaction();
    //! @}

    //! @name 查询执行控制
    //! @{
    /**
     * @brief 执行SQL查询
     * @param query SQL语句字符串
     * @param allow_stream_result 是否允许流式结果
     * @return 查询结果对象（可能是流式或物化结果）
     */
    DUCKDB_API unique_ptr<QueryResult> Query(const string &query, bool allow_stream_result);

    /**
     * @brief 创建异步查询
     * @param statement 已解析的SQL语句
     * @return 待处理查询结果指针
     */
    DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(unique_ptr<SQLStatement> statement, bool allow_stream_result);
    //! @}

    //! @name 预处理语句
    //! @{
    DUCKDB_API unique_ptr<PreparedStatement> Prepare(const string &query);
    DUCKDB_API unique_ptr<QueryResult> Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
                                              case_insensitive_map_t<BoundParameterData> &values);
    //! @}

    //! @name 元数据操作
    //! @{
    DUCKDB_API unique_ptr<TableDescription> TableInfo(const string &schema_name, const string &table_name);
    DUCKDB_API void Append(TableDescription &description, ColumnDataCollection &collection);
    //! @}

    //! @name 状态监控
    //! @{
    DUCKDB_API QueryProgress GetQueryProgress();
    DUCKDB_API bool ExecutionIsFinished();
    //! @}

    //! @name 高级功能
    //! @{
    DUCKDB_API void RegisterFunction(CreateFunctionInfo &info);  //!< 注册临时函数
    DUCKDB_API unique_ptr<LogicalOperator> ExtractPlan(const string &query); //!< 提取逻辑计划
    //! @}

private:
    //! @name 内部实现
    //! @{
    mutex context_lock;                       //!< 上下文操作互斥锁
    unique_ptr<ActiveQueryContext> active_query; //!< 当前活跃查询上下文
    QueryProgress query_progress;             //!< 查询进度跟踪
    connection_t connection_id;              //!< 客户端连接标识符

    // 内部执行方法
    unique_ptr<QueryResult> RunStatementInternal(ClientContextLock &lock, const string &query,
                                                unique_ptr<SQLStatement> statement, bool allow_stream_result);
    void CleanupInternal(ClientContextLock &lock, BaseQueryResult *result = nullptr);
    //! @}
};

class ClientContextLock {
public:
	explicit ClientContextLock(mutex &context_lock) : client_guard(context_lock) {
	}

	~ClientContextLock() {
	}

private:
	lock_guard<mutex> client_guard;
};

} // namespace duckdb
