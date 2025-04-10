//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/extra_operator_info.hpp"

namespace duckdb {
class DynamicTableFilterSet;

//! LogicalGet represents a scan operation from a data source
// LogicalGet 表示从数据源进行扫描操作的逻辑算子（查询计划中的表扫描节点）
class LogicalGet : public LogicalOperator {
public:
    // 算子类型标识
    static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_GET;

public:
    // 构造函数：初始化表扫描操作
    LogicalGet(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data,
               vector<LogicalType> returned_types, vector<string> returned_names,
               virtual_column_map_t virtual_columns = virtual_column_map_t());

    //! 当前绑定上下文中的表索引（用于唯一标识表）
    idx_t table_index;

    //! 表扫描函数（决定如何读取数据，如顺序扫描、索引扫描等）
    TableFunction function;

    //! 表扫描函数的绑定数据（包含扫描参数）
    unique_ptr<FunctionData> bind_data;

    //! 表函数能返回的所有列的类型
    vector<LogicalType> returned_types;

    //! 表函数能返回的所有列的名称
    vector<string> names;

    //! 虚拟列映射（如rowid等系统生成列）
    virtual_column_map_t virtual_columns;

    //! 外部实际使用的列索引（列裁剪优化）
    vector<idx_t> projection_ids;

    //! 下推到表扫描的过滤条件
    TableFilterSet table_filters;

    //! 表函数的输入参数值
    vector<Value> parameters;

    //! 表函数的命名参数（key-value形式）
    named_parameter_map_t named_parameters;

    //! 表输入输出函数的输入表类型
    vector<LogicalType> input_table_types;

    //! 表输入输出函数的输入表列名
    vector<string> input_table_names;

    //! 表输入输出函数的投影列索引
    vector<column_t> projected_input;

    //! 额外操作信息（如Hive分区过滤、采样率等说明性信息）
    ExtraOperatorInfo extra_info;

    //! 动态生成的表过滤器（例如来自连接操作的过滤条件）
    shared_ptr<DynamicTableFilterSet> dynamic_filters;

    // 获取算子名称（用于查询计划展示）
    string GetName() const override;

    // 参数转字符串（用于调试）
    InsertionOrderPreservingMap<string> ParamsToString() const override;

    //! 获取被扫描的基础表（可能为空）
    optional_ptr<TableCatalogEntry> GetTable() const;

    //! 获取任意一列（通常用于COUNT(*)等不关心具体列的场景）
    column_t GetAnyColumn() const;

    // 列类型/名称访问接口
    const LogicalType &GetColumnType(const ColumnIndex &column_index) const;
    const string &GetColumnName(const ColumnIndex &column_index) const;

public:
    // 列ID管理接口
    void SetColumnIds(vector<ColumnIndex> &&column_ids);
    void AddColumnId(column_t column_id);
    void ClearColumnIds();
    const vector<ColumnIndex> &GetColumnIds() const;
    vector<ColumnIndex> &GetMutableColumnIds();

    // 获取列绑定信息（用于查询计划生成）
    vector<ColumnBinding> GetColumnBindings() override;

    // 估算基数（行数）
    idx_t EstimateCardinality(ClientContext &context) override;

    // 获取关联的表索引
    vector<idx_t> GetTableIndex() const override;

    // 是否支持序列化（由表函数决定）
    bool SupportSerialization() const override {
        return function.verify_serialization;
    }

    // 序列化/反序列化接口
    void Serialize(Serializer &serializer) const override;
    static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
    // 解析输出列类型
    void ResolveTypes() override;

private:
    // 默认构造函数（仅供反序列化使用）
    LogicalGet();

private:
    //! 绑定的列ID列表（映射到物理存储的列标识）
    vector<ColumnIndex> column_ids;
};
} // namespace duckdb
