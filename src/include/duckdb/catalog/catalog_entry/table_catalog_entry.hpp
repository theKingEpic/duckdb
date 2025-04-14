//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/table_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/planner/bound_constraint.hpp"
#include "duckdb/storage/table/table_statistics.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/catalog/catalog_entry/table_column_type.hpp"
#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/common/table_column.hpp"

namespace duckdb {

class DataTable;

struct RenameColumnInfo;
struct RenameFieldInfo;
struct AddColumnInfo;
struct AddFieldInfo;
struct RemoveColumnInfo;
struct RemoveFieldInfo;
struct SetDefaultInfo;
struct ChangeColumnTypeInfo;
struct AlterForeignKeyInfo;
struct SetNotNullInfo;
struct DropNotNullInfo;
struct SetColumnCommentInfo;
struct CreateTableInfo;
struct BoundCreateTableInfo;

class TableFunction;
struct FunctionData;
struct EntryLookupInfo;

class Binder;
struct ColumnSegmentInfo;
class TableStorageInfo;

class LogicalGet;
class LogicalProjection;
class LogicalUpdate;
/**
 * @class TableCatalogEntry
 * @brief 表示目录中的表条目，继承自StandardEntry
 *
 * 该类提供了数据库目录系统中表相关操作的接口和实现
 */
class TableCatalogEntry : public StandardEntry {
public:
    /// @brief 表类型的目录类型标识符
    static constexpr const CatalogType Type = CatalogType::TABLE_ENTRY;//entry 条目
    //constexpr 是一个关键字，用于声明编译时常量或可在编译时求值的函数。它的核心特点是让表达式或函数在编译阶段就能确定结果，而不是等到运行时

    /// @brief 表类型的名称标识符
    static constexpr const char *Name = "table";

public:
    /**
     * @brief 构造函数，创建表目录条目并初始化存储
     * @param catalog 父目录的引用
     * @param schema 父模式的引用
     * @param info 包含表定义的CreateTableInfo对象
     */
    DUCKDB_API TableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);

public:
    /**
     * @brief 获取表的创建信息
     * @return 包含表定义的CreateInfo唯一指针
     */
    DUCKDB_API unique_ptr<CreateInfo> GetInfo() const override;

    /**
     * @brief 检查表是否包含生成列
     * @return 如果表有生成列返回true，否则返回false
     */
    DUCKDB_API bool HasGeneratedColumns() const;

    // 列操作
    /**
     * @brief 检查列是否存在
     * @param name 要检查的列名
     * @return 存在返回true，否则返回false
     */
    DUCKDB_API bool ColumnExists(const string &name) const;

    /**
     * @brief 通过列名获取列定义
     * @param name 要获取的列名
     * @return 列定义的常量引用
     * @throws 如果列不存在则抛出异常
     */
    DUCKDB_API const ColumnDefinition &GetColumn(const string &name) const;

    /**
     * @brief 通过逻辑索引获取列定义
     * @param idx 列的逻辑索引
     * @return 列定义的常量引用
     * @throws 如果列不存在则抛出异常
     */
    DUCKDB_API const ColumnDefinition &GetColumn(LogicalIndex idx) const;

    /**
     * @brief 获取表的类型列表（不包括生成列）
     * @return 包含所有列类型的vector
     */
    DUCKDB_API vector<LogicalType> GetTypes() const;

    /**
     * @brief 获取表的列列表
     * @return 列列表的常量引用
     */
    DUCKDB_API const ColumnList &GetColumns() const;

    /**
     * @brief 获取表的底层存储
     * @return 数据表的引用
     */
    virtual DataTable &GetStorage();

    // 约束操作
    /**
     * @brief 获取表的约束列表
     * @return 约束列表的常量引用
     */
    DUCKDB_API const vector<unique_ptr<Constraint>> &GetConstraints() const;

    /**
     * @brief 生成表的SQL定义语句
     * @return SQL语句字符串
     */
    DUCKDB_API string ToSQL() const override;

    // 统计信息
    /**
     * @brief 获取列的统计信息
     * @param context 客户端上下文
     * @param column_id 列ID
     * @return 基础统计信息的唯一指针
     */
    virtual unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) = 0;

    /**
     * @brief 获取表的采样器
     * @return 阻塞采样的唯一指针
     */
    virtual unique_ptr<BlockingSample> GetSample();

    // 列索引操作
    /**
     * @brief 通过列名获取列索引
     * @param name 列名
     * @param if_exists 如果为true，当列不存在时返回INVALID_INDEX而不抛出异常
     * @return 列的逻辑索引
     * @throws 如果列不存在且if_exists为false则抛出异常
     */
    DUCKDB_API LogicalIndex GetColumnIndex(string &name, bool if_exists = false) const;

    // 扫描功能
    /**
     * @brief 获取表的扫描函数
     * @param context 客户端上下文
     * @param bind_data 输出参数，存储绑定数据
     * @return 表扫描函数
     */
    virtual TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) = 0;

    /**
     * @brief 获取表的扫描函数（带查找信息）
     * @param context 客户端上下文
     * @param bind_data 输出参数，存储绑定数据
     * @param lookup_info 条目查找信息
     * @return 表扫描函数
     */
    virtual TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                        const EntryLookupInfo &lookup_info);

    /**
     * @brief 检查是否是DuckDB原生表
     * @return 默认返回false，派生类可覆盖
     */
    virtual bool IsDuckTable() const {
        return false;
    }

    // SQL生成
    /**
     * @brief 将列和约束转换为SQL语句
     * @param columns 列列表
     * @param constraints 约束列表
     * @return SQL语句字符串
     */
    DUCKDB_API static string ColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints);

    /**
     * @brief 将列名列表转换为SQL表达式
     * @param columns 列列表
     * @return 列名SQL表达式字符串，如(col1,col2,col3)
     */
    static string ColumnNamesToSQL(const ColumnList &columns);

    // 存储信息
    /**
     * @brief 获取表的列段信息
     * @return 列段信息列表
     */
    virtual vector<ColumnSegmentInfo> GetColumnSegmentInfo();

    /**
     * @brief 获取表的存储信息
     * @param context 客户端上下文
     * @return 表存储信息对象
     */
    virtual TableStorageInfo GetStorageInfo(ClientContext &context) = 0;

    // 约束绑定
    /**
     * @brief 绑定更新操作的约束
     * @param binder 绑定器
     * @param get 逻辑获取节点
     * @param proj 逻辑投影节点
     * @param update 逻辑更新节点
     * @param context 客户端上下文
     */
    virtual void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
                                    ClientContext &context);

    // 主键操作
    /**
     * @brief 获取表的主键约束
     * @return 主键约束的指针，如果没有则返回nullptr
     */
    optional_ptr<Constraint> GetPrimaryKey() const;

    /**
     * @brief 检查表是否有主键
     * @return 有主键返回true，否则返回false
     */
    bool HasPrimaryKey() const;

    // 虚拟列
    /**
     * @brief 获取表的虚拟列
     * @return 虚拟列映射表
     */
    virtual virtual_column_map_t GetVirtualColumns() const;

    /**
     * @brief 获取表的行ID列
     * @return 行ID列的列表
     */
    virtual vector<column_t> GetRowIdColumns() const;

protected:
    /// @brief 表的列列表
    ColumnList columns;

    /// @brief 表的约束列表
    vector<unique_ptr<Constraint>> constraints;
};
} // namespace duckdb
