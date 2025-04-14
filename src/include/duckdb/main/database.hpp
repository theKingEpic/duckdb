//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/main/capi/extension_api.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/main/extension_install_info.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/valid_checker.hpp"

namespace duckdb {
class BufferManager;
class DatabaseManager;
class StorageManager;
class Catalog;
class TransactionManager;
class ConnectionManager;
class FileSystem;
class TaskScheduler;
class ObjectCache;
struct AttachInfo;
struct AttachOptions;
class DatabaseFileSystem;
struct DatabaseCacheEntry;
class LogManager;

struct ExtensionInfo {
	bool is_loaded;
	unique_ptr<ExtensionInstallInfo> install_info;
	unique_ptr<ExtensionLoadedInfo> load_info;
};

/**
 * @class DatabaseInstance
 * @brief DuckDB 数据库实例的核心管理类
 *
 * 负责管理数据库生命周期、资源分配和组件协调，继承自 enable_shared_from_this 以支持智能指针安全访问。
 */
class DatabaseInstance : public enable_shared_from_this<DatabaseInstance> {
    friend class DuckDB; // 允许 DuckDB 主类访问私有成员

public:
    /// @name 构造/析构
    /// @{
    DUCKDB_API DatabaseInstance();  ///< 构造函数，初始化空数据库实例
    DUCKDB_API ~DatabaseInstance(); ///< 析构函数，释放所有资源
    /// @}

    DBConfig config; ///< 数据库全局配置项（内存限制、线程数等）

public:
    /// @name 核心组件访问
    /// @{
    BufferPool& GetBufferPool() const; ///< 获取内存缓冲池实例
    DUCKDB_API SecretManager& GetSecretManager(); ///< 获取密钥管理器
    DUCKDB_API BufferManager& GetBufferManager(); ///< 获取缓冲区管理器（读写协调）
    DUCKDB_API const BufferManager& GetBufferManager() const; ///< 常量版本
    DUCKDB_API DatabaseManager& GetDatabaseManager(); ///< 获取多数据库管理器
    DUCKDB_API FileSystem& GetFileSystem(); ///< 获取抽象文件系统接口
    DUCKDB_API TaskScheduler& GetScheduler(); ///< 获取并行任务调度器
    DUCKDB_API ObjectCache& GetObjectCache(); ///< 获取查询计划缓存
    DUCKDB_API ConnectionManager& GetConnectionManager(); ///< 获取客户端连接管理器
    DUCKDB_API ValidChecker& GetValidChecker(); ///< 获取实例状态验证器
    DUCKDB_API LogManager& GetLogManager() const; ///< 获取日志记录器
    /// @}

    /// @name 扩展管理
    /// @{
    /**
     * @brief 标记扩展加载完成
     * @param extension_name 扩展名称
     * @param install_info 安装信息
     */
    DUCKDB_API void SetExtensionLoaded(const string& extension_name, ExtensionInstallInfo& install_info);

    DUCKDB_API const unordered_map<string, ExtensionInfo>& GetExtensions(); ///< 获取已加载扩展列表
    DUCKDB_API bool ExtensionIsLoaded(const string& name); ///< 检查扩展是否加载
    /// @}

    /// @name 运行时功能
    /// @{
    DUCKDB_API SettingLookupResult TryGetCurrentSetting(const string& key, Value& result) const; ///< 查询配置项值
    idx_t NumberOfThreads(); ///< 返回当前配置的线程数
    /// @}

    /// @name 数据库操作
    /// @{
    /**
     * @brief 创建附加数据库
     * @param context 客户端上下文
     * @param info 附加配置信息
     * @param options 附加选项
     * @return 附加数据库句柄
     */
    unique_ptr<AttachedDatabase> CreateAttachedDatabase(ClientContext& context, const AttachInfo& info,
                                                        const AttachOptions& options);
    /// @}

private:
    /// @name 初始化方法
    /// @{
    void Initialize(const char* path, DBConfig* config); ///< 初始化数据库实例
    void LoadExtensionSettings(); ///< 加载扩展相关配置
    void CreateMainDatabase(); ///< 创建主数据库
    void Configure(DBConfig& config, const char* path); ///< 应用配置项
    /// @}

private:
    /// @name 核心组件实例
    /// @{
    shared_ptr<BufferManager> buffer_manager;          ///< 缓冲区管理器
    unique_ptr<DatabaseManager> db_manager;           ///< 数据库管理器
    unique_ptr<TaskScheduler> scheduler;              ///< 任务调度器
    unique_ptr<ObjectCache> object_cache;             ///< 对象缓存
    unique_ptr<ConnectionManager> connection_manager; ///< 连接管理器
    unordered_map<string, ExtensionInfo> loaded_extensions_info; ///< 已加载扩展信息
    ValidChecker db_validity;                        ///< 实例有效性检查器
    unique_ptr<DatabaseFileSystem> db_file_system;   ///< 专用文件系统
    shared_ptr<LogManager> log_manager;              ///< 日志管理器
    /// @}

    duckdb_ext_api_v1 (*create_api_v1)(); ///< 扩展API创建函数指针
};

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class DuckDB {
public:
	DUCKDB_API explicit DuckDB(const char *path = nullptr, DBConfig *config = nullptr);
	DUCKDB_API explicit DuckDB(const string &path, DBConfig *config = nullptr);
	DUCKDB_API explicit DuckDB(DatabaseInstance &instance);

	DUCKDB_API ~DuckDB();

	//! Reference to the actual database instance
	shared_ptr<DatabaseInstance> instance;

public:
	// Load a statically loaded extension by its class
	template <class T>
	void LoadStaticExtension() {
		T extension;
		if (ExtensionIsLoaded(extension.Name())) {
			return;
		}
		extension.Load(*this);
		ExtensionInstallInfo install_info;
		install_info.mode = ExtensionInstallMode::STATICALLY_LINKED;
		install_info.version = extension.Version();
		instance->SetExtensionLoaded(extension.Name(), install_info);
	}

	// DEPRECATED function that some extensions may still use to call their own Load method from the
	// _init function of their loadable extension. Don't use this. Instead opt for a static LoadInternal function called
	// from both the _init function and the Extension::Load. (see autocomplete extension)
	// TODO: when to remove this function?
	template <class T>
	void LoadExtension() {
		T extension;
		extension.Load(*this);
	}

	DUCKDB_API FileSystem &GetFileSystem();

	DUCKDB_API idx_t NumberOfThreads();
	DUCKDB_API static const char *SourceID();
	DUCKDB_API static const char *LibraryVersion();
	DUCKDB_API static idx_t StandardVectorSize();
	DUCKDB_API static string Platform();
	DUCKDB_API bool ExtensionIsLoaded(const string &name);
};

} // namespace duckdb
