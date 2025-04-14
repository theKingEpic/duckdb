//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/table_column.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

#include <functional>

namespace duckdb {

class BaseStatistics;
class LogicalDependencyList;
class LogicalGet;
class TableFunction;
class TableFilterSet;
class TableFunctionRef;
class TableCatalogEntry;
class SampleOptions;
struct MultiFileReader;
struct OperatorPartitionData;
struct OperatorPartitionInfo;

struct TableFunctionInfo {
	DUCKDB_API virtual ~TableFunctionInfo();

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

struct GlobalTableFunctionState {
public:
	// value returned from MaxThreads when as many threads as possible should be used
	constexpr static const int64_t MAX_THREADS = 999999999;

public:
	DUCKDB_API virtual ~GlobalTableFunctionState();

	virtual idx_t MaxThreads() const {
		return 1;
	}

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

struct LocalTableFunctionState {
	DUCKDB_API virtual ~LocalTableFunctionState();

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

struct TableFunctionBindInput {
	TableFunctionBindInput(vector<Value> &inputs, named_parameter_map_t &named_parameters,
	                       vector<LogicalType> &input_table_types, vector<string> &input_table_names,
	                       optional_ptr<TableFunctionInfo> info, optional_ptr<Binder> binder,
	                       TableFunction &table_function, const TableFunctionRef &ref)
	    : inputs(inputs), named_parameters(named_parameters), input_table_types(input_table_types),
	      input_table_names(input_table_names), info(info), binder(binder), table_function(table_function), ref(ref) {
	}

	vector<Value> &inputs;
	named_parameter_map_t &named_parameters;
	vector<LogicalType> &input_table_types;
	vector<string> &input_table_names;
	optional_ptr<TableFunctionInfo> info;
	optional_ptr<Binder> binder;
	TableFunction &table_function;
	const TableFunctionRef &ref;
};

struct TableFunctionInitInput {
	TableFunctionInitInput(optional_ptr<const FunctionData> bind_data_p, vector<column_t> column_ids_p,
	                       const vector<idx_t> &projection_ids_p, optional_ptr<TableFilterSet> filters_p,
	                       optional_ptr<SampleOptions> sample_options_p = nullptr)
	    : bind_data(bind_data_p), column_ids(std::move(column_ids_p)), projection_ids(projection_ids_p),
	      filters(filters_p), sample_options(sample_options_p) {
		for (auto &col_id : column_ids) {
			column_indexes.emplace_back(col_id);
		}
	}

	TableFunctionInitInput(optional_ptr<const FunctionData> bind_data_p, vector<ColumnIndex> column_indexes_p,
	                       const vector<idx_t> &projection_ids_p, optional_ptr<TableFilterSet> filters_p,
	                       optional_ptr<SampleOptions> sample_options_p = nullptr)
	    : bind_data(bind_data_p), column_indexes(std::move(column_indexes_p)), projection_ids(projection_ids_p),
	      filters(filters_p), sample_options(sample_options_p) {
		for (auto &col_id : column_indexes) {
			column_ids.emplace_back(col_id.GetPrimaryIndex());
		}
	}

	optional_ptr<const FunctionData> bind_data;
	vector<column_t> column_ids;
	vector<ColumnIndex> column_indexes;
	const vector<idx_t> projection_ids;
	optional_ptr<TableFilterSet> filters;
	optional_ptr<SampleOptions> sample_options;

	bool CanRemoveFilterColumns() const {
		if (projection_ids.empty()) {
			// No filter columns to remove.
			return false;
		}
		if (projection_ids.size() == column_ids.size()) {
			// Filter column is used in remainder of plan, so we cannot remove it.
			return false;
		}
		// Fewer columns need to be projected out than that we scan.
		return true;
	}
};

struct TableFunctionInput {
public:
	TableFunctionInput(optional_ptr<const FunctionData> bind_data_p,
	                   optional_ptr<LocalTableFunctionState> local_state_p,
	                   optional_ptr<GlobalTableFunctionState> global_state_p)
	    : bind_data(bind_data_p), local_state(local_state_p), global_state(global_state_p) {
	}

public:
	optional_ptr<const FunctionData> bind_data;
	optional_ptr<LocalTableFunctionState> local_state;
	optional_ptr<GlobalTableFunctionState> global_state;
};

struct TableFunctionPartitionInput {
	TableFunctionPartitionInput(optional_ptr<const FunctionData> bind_data_p, const vector<column_t> &partition_ids)
	    : bind_data(bind_data_p), partition_ids(partition_ids) {
	}

	optional_ptr<const FunctionData> bind_data;
	const vector<column_t> &partition_ids;
};

struct TableFunctionToStringInput {
	TableFunctionToStringInput(const TableFunction &table_function_p, optional_ptr<const FunctionData> bind_data_p)
	    : table_function(table_function_p), bind_data(bind_data_p) {
	}
	const TableFunction &table_function;
	optional_ptr<const FunctionData> bind_data;
};

struct TableFunctionDynamicToStringInput {
	TableFunctionDynamicToStringInput(const TableFunction &table_function_p,
	                                  optional_ptr<const FunctionData> bind_data_p,
	                                  optional_ptr<LocalTableFunctionState> local_state_p,
	                                  optional_ptr<GlobalTableFunctionState> global_state_p)
	    : table_function(table_function_p), bind_data(bind_data_p), local_state(local_state_p),
	      global_state(global_state_p) {
	}
	const TableFunction &table_function;
	optional_ptr<const FunctionData> bind_data;
	optional_ptr<LocalTableFunctionState> local_state;
	optional_ptr<GlobalTableFunctionState> global_state;
};

struct TableFunctionGetPartitionInput {
public:
	TableFunctionGetPartitionInput(optional_ptr<const FunctionData> bind_data_p,
	                               optional_ptr<LocalTableFunctionState> local_state_p,
	                               optional_ptr<GlobalTableFunctionState> global_state_p,
	                               const OperatorPartitionInfo &partition_info_p)
	    : bind_data(bind_data_p), local_state(local_state_p), global_state(global_state_p),
	      partition_info(partition_info_p) {
	}

public:
	optional_ptr<const FunctionData> bind_data;
	optional_ptr<LocalTableFunctionState> local_state;
	optional_ptr<GlobalTableFunctionState> global_state;
	const OperatorPartitionInfo &partition_info;
};

struct GetPartitionStatsInput {
	GetPartitionStatsInput(const TableFunction &table_function_p, optional_ptr<const FunctionData> bind_data_p)
	    : table_function(table_function_p), bind_data(bind_data_p) {
	}

	const TableFunction &table_function;
	optional_ptr<const FunctionData> bind_data;
};

enum class ScanType : uint8_t { TABLE, PARQUET, EXTERNAL };

struct BindInfo {
public:
	explicit BindInfo(ScanType type_p) : type(type_p) {};
	explicit BindInfo(TableCatalogEntry &table) : type(ScanType::TABLE), table(&table) {};

	unordered_map<string, Value> options;
	ScanType type;
	optional_ptr<TableCatalogEntry> table;

	void InsertOption(const string &name, Value value) { // NOLINT: work-around bug in clang-tidy
		if (options.find(name) != options.end()) {
			throw InternalException("This option already exists");
		}
		options.emplace(name, std::move(value));
	}
	template <class T>
	T GetOption(const string &name) {
		if (options.find(name) == options.end()) {
			throw InternalException("This option does not exist");
		}
		return options[name].GetValue<T>();
	}
	template <class T>
	vector<T> GetOptionList(const string &name) {
		if (options.find(name) == options.end()) {
			throw InternalException("This option does not exist");
		}
		auto option = options[name];
		if (option.type().id() != LogicalTypeId::LIST) {
			throw InternalException("This option is not a list");
		}
		vector<T> result;
		auto list_children = ListValue::GetChildren(option);
		for (auto &child : list_children) {
			result.emplace_back(child.GetValue<T>());
		}
		return result;
	}
};

typedef unique_ptr<FunctionData> (*table_function_bind_t)(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names);
typedef unique_ptr<TableRef> (*table_function_bind_replace_t)(ClientContext &context, TableFunctionBindInput &input);
typedef unique_ptr<GlobalTableFunctionState> (*table_function_init_global_t)(ClientContext &context,
                                                                             TableFunctionInitInput &input);
typedef unique_ptr<LocalTableFunctionState> (*table_function_init_local_t)(ExecutionContext &context,
                                                                           TableFunctionInitInput &input,
                                                                           GlobalTableFunctionState *global_state);
typedef unique_ptr<BaseStatistics> (*table_statistics_t)(ClientContext &context, const FunctionData *bind_data,
                                                         column_t column_index);
typedef void (*table_function_t)(ClientContext &context, TableFunctionInput &data, DataChunk &output);
typedef OperatorResultType (*table_in_out_function_t)(ExecutionContext &context, TableFunctionInput &data,
                                                      DataChunk &input, DataChunk &output);
typedef OperatorFinalizeResultType (*table_in_out_function_final_t)(ExecutionContext &context, TableFunctionInput &data,
                                                                    DataChunk &output);
typedef OperatorPartitionData (*table_function_get_partition_data_t)(ClientContext &context,
                                                                     TableFunctionGetPartitionInput &input);

typedef BindInfo (*table_function_get_bind_info_t)(const optional_ptr<FunctionData> bind_data);

typedef unique_ptr<MultiFileReader> (*table_function_get_multi_file_reader_t)(const TableFunction &);

typedef bool (*table_function_supports_pushdown_type_t)(const LogicalType &type);

typedef double (*table_function_progress_t)(ClientContext &context, const FunctionData *bind_data,
                                            const GlobalTableFunctionState *global_state);
typedef void (*table_function_dependency_t)(LogicalDependencyList &dependencies, const FunctionData *bind_data);
typedef unique_ptr<NodeStatistics> (*table_function_cardinality_t)(ClientContext &context,
                                                                   const FunctionData *bind_data);
typedef void (*table_function_pushdown_complex_filter_t)(ClientContext &context, LogicalGet &get,
                                                         FunctionData *bind_data,
                                                         vector<unique_ptr<Expression>> &filters);
typedef InsertionOrderPreservingMap<string> (*table_function_to_string_t)(TableFunctionToStringInput &input);
typedef InsertionOrderPreservingMap<string> (*table_function_dynamic_to_string_t)(
    TableFunctionDynamicToStringInput &input);

typedef void (*table_function_serialize_t)(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                           const TableFunction &function);
typedef unique_ptr<FunctionData> (*table_function_deserialize_t)(Deserializer &deserializer, TableFunction &function);

typedef void (*table_function_type_pushdown_t)(ClientContext &context, optional_ptr<FunctionData> bind_data,
                                               const unordered_map<idx_t, LogicalType> &new_column_types);
typedef TablePartitionInfo (*table_function_get_partition_info_t)(ClientContext &context,
                                                                  TableFunctionPartitionInput &input);

typedef vector<PartitionStatistics> (*table_function_get_partition_stats_t)(ClientContext &context,
                                                                            GetPartitionStatsInput &input);

typedef virtual_column_map_t (*table_function_get_virtual_columns_t)(ClientContext &context,
                                                                     optional_ptr<FunctionData> bind_data);

//! When to call init_global to initialize the table function
enum class TableFunctionInitialization { INITIALIZE_ON_EXECUTE, INITIALIZE_ON_SCHEDULE };

/**
 * @class TableFunction
 * @brief DuckDB 表函数基类，用于定义可扩展的表扫描操作
 *
 * 继承自 SimpleNamedParameterFunction，支持带参数的表格数据处理，
 * 提供完整的生命周期管理（绑定、初始化、执行、统计等）。
 */
class TableFunction : public SimpleNamedParameterFunction {
public:
    /// @name 构造函数
    /// @{
    /**
     * @brief 构造表函数（指定名称和参数类型）
     * @param name 函数名称
     * @param arguments 输入参数类型列表
     * @param function 主执行函数
     * @param bind 绑定函数（可选）
     * @param init_global 全局初始化函数（可选）
     * @param init_local 线程本地初始化函数（可选）
     */
    DUCKDB_API TableFunction(string name, vector<LogicalType> arguments,
                           table_function_t function,
                           table_function_bind_t bind = nullptr,
                           table_function_init_global_t init_global = nullptr,
                           table_function_init_local_t init_local = nullptr);

    /**
     * @brief 构造表函数（省略名称，使用默认名称）
     * @param arguments 输入参数类型列表
     * @param function 主执行函数
     * @param bind 绑定函数（可选）
     * @param init_global 全局初始化函数（可选）
     * @param init_local 线程本地初始化函数（可选）
     */
    DUCKDB_API TableFunction(const vector<LogicalType> &arguments,
                           table_function_t function,
                           table_function_bind_t bind = nullptr,
                           table_function_init_global_t init_global = nullptr,
                           table_function_init_local_t init_local = nullptr);

    DUCKDB_API TableFunction(); ///< 默认构造函数
    /// @}

    /// @name 核心函数指针
    /// @{
    table_function_bind_t bind; ///< 绑定函数（确定返回类型和绑定数据）
    table_function_bind_replace_t bind_replace; ///< （可选）绑定替换函数，用于生成逻辑计划
    table_function_init_global_t init_global; ///< （可选）全局状态初始化函数
    table_function_init_local_t init_local; ///< （可选）线程本地状态初始化函数
    table_function_t function; ///< 主执行函数
    table_in_out_function_t in_out_function; ///< （可选）输入输出处理函数
    table_in_out_function_final_t in_out_function_final; ///< （可选）输入输出最终处理函数
    /// @}

    /// @name 统计与优化
    /// @{
    table_statistics_t statistics; ///< （可选）列统计信息获取函数
    table_function_dependency_t dependency; ///< （可选）依赖关系声明函数
    table_function_cardinality_t cardinality; ///< （可选）基数估算函数
    /// @}

    /// @name 高级功能
    /// @{
    table_function_pushdown_complex_filter_t pushdown_complex_filter; ///< （可选）复杂谓词下推支持
    table_function_to_string_t to_string; ///< （可选）执行前字符串化函数（用于EXPLAIN）
    table_function_dynamic_to_string_t dynamic_to_string; ///< （可选）执行后字符串化函数（用于PROFILE）
    table_function_progress_t table_scan_progress; ///< （可选）扫描进度报告函数
    table_function_get_partition_data_t get_partition_data; ///< （可选）分区数据获取函数
    table_function_get_bind_info_t get_bind_info; ///< （可选）绑定扩展信息获取函数
    table_function_type_pushdown_t type_pushdown; ///< （可选）类型下推支持函数
    table_function_get_multi_file_reader_t get_multi_file_reader; ///< （可选）多文件阅读器注入函数
    table_function_supports_pushdown_type_t supports_pushdown_type; ///< （可选）类型敏感的下推支持
    table_function_get_partition_info_t get_partition_info; ///< （可选）分区信息获取函数
    table_function_get_partition_stats_t get_partition_stats; ///< （可选）分区统计获取函数
    table_function_get_virtual_columns_t get_virtual_columns; ///< （可选）虚拟列获取函数
    /// @}

    /// @name 序列化
    /// @{
    table_function_serialize_t serialize; ///< （可选）状态序列化函数
    table_function_deserialize_t deserialize; ///< （可选）状态反序列化函数
    bool verify_serialization = true; ///< 是否验证序列化结果（默认true）
    /// @}

    /// @name 下推优化开关
    /// @{
    bool projection_pushdown; ///< 是否支持列投影下推（默认false）
    bool filter_pushdown; ///< 是否支持谓词下推（默认false）
    bool filter_prune; ///< 是否支持过滤列剪枝（默认false）
    bool sampling_pushdown; ///< 是否支持采样下推（默认false）
    bool late_materialization; ///< 是否支持延迟物化（默认false）
    /// @}

    /// @name 其他成员
    /// @{
    shared_ptr<TableFunctionInfo> function_info; ///< 附加函数信息（传递给bind）
    TableFunctionInitialization global_initialization =
        TableFunctionInitialization::INITIALIZE_ON_EXECUTE; ///< 全局初始化时机（默认执行时初始化）
    /// @}

    DUCKDB_API bool Equal(const TableFunction &rhs) const; ///< 比较两个表函数是否等效
};

} // namespace duckdb
