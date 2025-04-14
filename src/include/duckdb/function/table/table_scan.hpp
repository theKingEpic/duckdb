//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/table_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {
class DuckTableEntry;
class TableCatalogEntry;

/**
 * @brief 表扫描绑定数据结构体，继承自TableFunctionData基类
 *
 * 该结构体用于存储表扫描操作所需的上下文信息和状态
 */
struct TableScanBindData : public TableFunctionData {
	/**
	 * @brief 显式构造函数
	 * @param table 要扫描的表目录条目引用
	 * @note 初始化时将is_index_scan和is_create_index设为false
	 */
	explicit TableScanBindData(TableCatalogEntry &table)
	    : table(table), is_index_scan(false), is_create_index(false) {
	}

	//! @brief 要扫描的表目录条目引用
	//! @details 存储对目标表的引用，用于访问表结构和元数据
	TableCatalogEntry &table;

	//! @brief 是否使用索引扫描的标志位
	//! @details 原字段用途已被弃用，现在用于在ANALYZE调用中表示索引扫描
	//! 当选择索引扫描时，通过const-cast修改此标志为true
	bool is_index_scan;

	//! @brief 是否为创建索引而进行的表扫描
	//! @details 标志位，用于区分普通扫描和索引创建时的特殊扫描
	bool is_create_index;

public:
	/**
	 * @brief 比较函数，用于判断两个绑定数据是否相等
	 * @param other_p 要比较的FunctionData基类引用
	 * @return bool 返回true表示两个绑定数据相等(比较的是表引用)
	 */
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TableScanBindData>();
		return &other.table == &table;
	}

	/**
	 * @brief 创建绑定数据的深拷贝
	 * @return unique_ptr<FunctionData> 返回包含拷贝数据的所有权指针
	 * @details 复制所有成员变量，包括表引用和标志位状态
	 */
	unique_ptr<FunctionData> Copy() const override {
		auto bind_data = make_uniq<TableScanBindData>(table);
		bind_data->is_index_scan = is_index_scan;
		bind_data->is_create_index = is_create_index;
		bind_data->column_ids = column_ids;
		return std::move(bind_data);
	}
};

//! The table scan function represents a sequential or index scan over one of DuckDB's base tables.
struct TableScanFunction {
	static void RegisterFunction(BuiltinFunctions &set);
	static TableFunction GetFunction();
};

} // namespace duckdb
