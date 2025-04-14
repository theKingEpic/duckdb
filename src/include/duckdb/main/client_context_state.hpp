//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/prepared_statement_mode.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include <mutex>

namespace duckdb {
class ClientContext;
class ErrorData;
class MetaTransaction;
class PreparedStatementData;
class SQLStatement;
struct PendingQueryParameters;
class RegisteredStateManager;

enum class RebindQueryInfo { DO_NOT_REBIND, ATTEMPT_TO_REBIND };

struct PreparedStatementCallbackInfo {
	PreparedStatementCallbackInfo(PreparedStatementData &prepared_statement, const PendingQueryParameters &parameters)
	    : prepared_statement(prepared_statement), parameters(parameters) {
	}

	PreparedStatementData &prepared_statement;
	const PendingQueryParameters &parameters;
};

//! ClientContextState is virtual base class for ClientContext-local (or Query-Local, using QueryEnd callback) state
//! e.g. caches that need to live as long as a ClientContext or Query.
class ClientContextState {
public:
	virtual ~ClientContextState() = default;
	virtual void QueryBegin(ClientContext &context) {
	}
	virtual void QueryEnd() {
	}
	virtual void QueryEnd(ClientContext &context) {
		QueryEnd();
	}
	virtual void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) {
		QueryEnd(context);
	}
	virtual void TransactionBegin(MetaTransaction &transaction, ClientContext &context) {
	}
	virtual void TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	}
	virtual void TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	}
	virtual void TransactionRollback(MetaTransaction &transaction, ClientContext &context,
	                                 optional_ptr<ErrorData> error) {
		TransactionRollback(transaction, context);
	}
	virtual bool CanRequestRebind() {
		return false;
	}
	virtual RebindQueryInfo OnPlanningError(ClientContext &context, SQLStatement &statement, ErrorData &error) {
		return RebindQueryInfo::DO_NOT_REBIND;
	}
	virtual RebindQueryInfo OnFinalizePrepare(ClientContext &context, PreparedStatementData &prepared_statement,
	                                          PreparedStatementMode mode) {
		return RebindQueryInfo::DO_NOT_REBIND;
	}
	virtual RebindQueryInfo OnExecutePrepared(ClientContext &context, PreparedStatementCallbackInfo &info,
	                                          RebindQueryInfo current_rebind) {
		return RebindQueryInfo::DO_NOT_REBIND;
	}
	virtual void WriteProfilingInformation(std::ostream &ss) {
	}
	virtual void OnTaskStart(ClientContext &context) {
	}
	virtual void OnTaskStop(ClientContext &context) {
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

/**
 * @class RegisteredStateManager
 * @brief 客户端上下文状态管理器
 *
 * 线程安全的注册表模式实现，用于管理ClientContext的扩展状态。
 * 支持状态的创建、获取、删除和遍历操作。
 */
class RegisteredStateManager {
public:
    /**
     * @brief 获取或创建指定键的状态对象（模板方法）
     * @tparam T 状态类型，需继承自ClientContextState
     * @tparam ARGS 构造参数类型
     * @param key 状态唯一标识键
     * @param args 状态对象构造参数
     * @return shared_ptr<T> 状态对象的共享指针
     *
     * @note 线程安全操作，当键不存在时自动构造新对象
     * @example
     * auto state = manager.GetOrCreate<MyState>("cache", arg1, arg2);
     */
    template <class T, typename... ARGS>
    shared_ptr<T> GetOrCreate(const string &key, ARGS &&... args) {
        lock_guard<mutex> l(lock);
        auto lookup = registered_state.find(key);
        if (lookup != registered_state.end()) {
            return shared_ptr_cast<ClientContextState, T>(lookup->second);
        }
        auto cache = make_shared_ptr<T>(std::forward<ARGS>(args)...);
        registered_state[key] = cache;
        return cache;
    }

    /**
     * @brief 获取指定键的状态对象（模板方法）
     * @tparam T 状态类型，需继承自ClientContextState
     * @param key 状态唯一标识键
     * @return shared_ptr<T> 状态对象的共享指针，键不存在时返回nullptr
     */
    template <class T>
    shared_ptr<T> Get(const string &key) {
        lock_guard<mutex> l(lock);
        auto lookup = registered_state.find(key);
        if (lookup == registered_state.end()) {
            return nullptr;
        }
        return shared_ptr_cast<ClientContextState, T>(lookup->second);
    }

    /**
     * @brief 注册新状态对象
     * @param key 状态唯一标识键
     * @param state_p 状态对象指针
     *
     * @warning 若键已存在会覆盖原有状态
     */
    void Insert(const string &key, shared_ptr<ClientContextState> state_p) {
        lock_guard<mutex> l(lock);
        registered_state.insert(make_pair(key, std::move(state_p)));
    }

    /**
     * @brief 移除指定键的状态对象
     * @param key 要移除的状态键
     */
    void Remove(const string &key) {
        lock_guard<mutex> l(lock);
        registered_state.erase(key);
    }

    /**
     * @brief 获取所有状态对象副本
     * @return vector<shared_ptr<ClientContextState>> 状态对象列表
     *
     * @note 返回列表是当前状态的快照，线程安全但可能立即过期
     */
    vector<shared_ptr<ClientContextState>> States() {
        lock_guard<mutex> l(lock);
        vector<shared_ptr<ClientContextState>> states;
        for (auto &entry : registered_state) {
            states.push_back(entry.second);
        }
        return states;
    }

private:
    mutex lock; ///< 状态访问互斥锁
    unordered_map<string, shared_ptr<ClientContextState>> registered_state; ///< 状态存储字典
};
} // namespace duckdb
