#include "duckdb/planner/planner.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

Planner::Planner(ClientContext &context) : binder(Binder::CreateBinder(context)), context(context) {
}

static void CheckTreeDepth(const LogicalOperator &op, idx_t max_depth, idx_t depth = 0) {
	if (depth >= max_depth) {
		throw ParserException("Maximum tree depth of %lld exceeded in logical planner", max_depth);
	}
	for (auto &child : op.children) {
		CheckTreeDepth(*child, max_depth, depth + 1);
	}
}

void Planner::CreatePlan(SQLStatement &statement) {
    // 获取查询分析器实例
    auto &profiler = QueryProfiler::Get(context);
    // 获取SQL语句中命名参数的数量
    auto parameter_count = statement.named_param_map.size();

    // 创建绑定参数映射，用于存储参数数据
	// 使用参数数据(parameter_data)初始化BoundParameterMap对象
	// 该对象用于存储和管理SQL语句中的绑定参数
    BoundParameterMap bound_parameters(parameter_data);

    // 标记参数是否已成功解析
    bool parameters_resolved = true;
    try {
        // 开始绑定阶段性能分析
        profiler.StartPhase(MetricsType::PLANNER_BINDING);
        // 设置绑定器的参数映射
        binder->parameters = &bound_parameters;
        // 执行绑定操作，将SQL语句转换为绑定后的语句
        auto bound_statement = binder->Bind(statement);
        // 结束绑定阶段性能分析
        profiler.EndPhase();

        // 保存绑定后的结果：列名、类型和执行计划
        this->names = bound_statement.names;
        this->types = bound_statement.types;
        this->plan = std::move(bound_statement.plan);

        // 检查执行计划的表达式深度是否超过配置的最大深度
        auto max_tree_depth = ClientConfig::GetConfig(context).max_expression_depth;
        CheckTreeDepth(*plan, max_tree_depth);
    } catch (const std::exception &ex) {
        // 处理绑定过程中出现的异常
        ErrorData error(ex);
        this->plan = nullptr;
        if (error.Type() == ExceptionType::PARAMETER_NOT_RESOLVED) {
            // 参数类型无法解析的情况
            this->names = {"unknown"};
            this->types = {LogicalTypeId::UNKNOWN};
            parameters_resolved = false;
        } else if (error.Type() != ExceptionType::INVALID) {
            // 其他类型的异常，尝试使用操作符扩展来处理
            auto &config = DBConfig::GetConfig(context);
            for (auto &extension_op : config.operator_extensions) {
                // 尝试让每个扩展操作符处理该语句
                auto bound_statement =
                    extension_op->Bind(context, *this->binder, extension_op->operator_info.get(), statement);
                if (bound_statement.plan != nullptr) {
                    // 如果扩展操作符成功处理，保存结果
                    this->names = bound_statement.names;
                    this->types = bound_statement.types;
                    this->plan = std::move(bound_statement.plan);
                    break;
                }
            }
            if (!this->plan) {
                // 如果没有扩展操作符能处理该语句，重新抛出异常
                throw;
            }
        } else {
            // 无效异常类型，直接重新抛出
            throw;
        }
    }

    // 获取语句属性并设置参数相关信息
    this->properties = binder->GetStatementProperties();
    this->properties.parameter_count = parameter_count;
    properties.bound_all_parameters = !bound_parameters.rebind && parameters_resolved;

    // 验证执行计划和参数
    Planner::VerifyPlan(context, plan, bound_parameters.GetParametersPtr());

    // 设置参数编号到参数值的映射
    for (auto &kv : bound_parameters.GetParameters()) {
        auto &identifier = kv.first;
        auto &param = kv.second;
        // 检查参数类型是否已解析
        if (!param->return_type.IsValid()) {
            properties.bound_all_parameters = false;
            continue;
        }
        // 设置参数的默认值
        param->SetValue(Value(param->return_type));
        value_map[identifier] = param;
    }
}

shared_ptr<PreparedStatementData> Planner::PrepareSQLStatement(unique_ptr<SQLStatement> statement) {
	auto copied_statement = statement->Copy();
	// create a plan of the underlying statement
	CreatePlan(std::move(statement));
	// now create the logical prepare
	auto prepared_data = make_shared_ptr<PreparedStatementData>(copied_statement->type);
	prepared_data->unbound_statement = std::move(copied_statement);
	prepared_data->names = names;
	prepared_data->types = types;
	prepared_data->value_map = std::move(value_map);
	prepared_data->properties = properties;
	return prepared_data;
}

void Planner::CreatePlan(unique_ptr<SQLStatement> statement) {
	D_ASSERT(statement);
	switch (statement->type) {
	case StatementType::SELECT_STATEMENT:
	case StatementType::INSERT_STATEMENT:
	case StatementType::COPY_STATEMENT:
	case StatementType::DELETE_STATEMENT:
	case StatementType::UPDATE_STATEMENT:
	case StatementType::CREATE_STATEMENT:
	case StatementType::DROP_STATEMENT:
	case StatementType::ALTER_STATEMENT:
	case StatementType::TRANSACTION_STATEMENT:
	case StatementType::EXPLAIN_STATEMENT:
	case StatementType::VACUUM_STATEMENT:
	case StatementType::RELATION_STATEMENT:
	case StatementType::CALL_STATEMENT:
	case StatementType::EXPORT_STATEMENT:
	case StatementType::PRAGMA_STATEMENT:
	case StatementType::SET_STATEMENT:
	case StatementType::LOAD_STATEMENT:
	case StatementType::EXTENSION_STATEMENT:
	case StatementType::PREPARE_STATEMENT:
	case StatementType::EXECUTE_STATEMENT:
	case StatementType::LOGICAL_PLAN_STATEMENT:
	case StatementType::ATTACH_STATEMENT:
	case StatementType::DETACH_STATEMENT:
	case StatementType::COPY_DATABASE_STATEMENT:
	case StatementType::UPDATE_EXTENSIONS_STATEMENT:
		CreatePlan(*statement);
		break;
	default:
		throw NotImplementedException("Cannot plan statement of type %s!", StatementTypeToString(statement->type));
	}
}

static bool OperatorSupportsSerialization(LogicalOperator &op) {
	for (auto &child : op.children) {
		if (!OperatorSupportsSerialization(*child)) {
			return false;
		}
	}
	return op.SupportSerialization();
}

void Planner::VerifyPlan(ClientContext &context, unique_ptr<LogicalOperator> &op,
                         optional_ptr<bound_parameter_map_t> map) {
	auto &config = DBConfig::GetConfig(context);
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	{
		auto &serialize_comp = config.options.serialization_compatibility;
		auto latest_version = SerializationCompatibility::Latest();
		if (serialize_comp.manually_set &&
		    serialize_comp.serialization_version != latest_version.serialization_version) {
			// Serialization should not be skipped, this test relies on the serialization to remove certain fields for
			// compatibility with older versions. This might change behavior, not doing this might make this test fail.
		} else {
			// if alternate verification is enabled we run the original operator
			return;
		}
	}
#endif
	if (!op || !ClientConfig::GetConfig(context).verify_serializer) {
		return;
	}
	//! SELECT only for now
	if (!OperatorSupportsSerialization(*op)) {
		return;
	}
	// verify the column bindings of the plan
	ColumnBindingResolver::Verify(*op);

	// format (de)serialization of this operator
	try {
		MemoryStream stream(Allocator::Get(context));

		SerializationOptions options;
		if (config.options.serialization_compatibility.manually_set) {
			// Override the default of 'latest' if this was manually set (for testing, mostly)
			options.serialization_compatibility = config.options.serialization_compatibility;
		} else {
			options.serialization_compatibility = SerializationCompatibility::Latest();
		}

		BinarySerializer::Serialize(*op, stream, options);
		stream.Rewind();
		bound_parameter_map_t parameters;
		auto new_plan = BinaryDeserializer::Deserialize<LogicalOperator>(stream, context, parameters);

		if (map) {
			*map = std::move(parameters);
		}
		op = std::move(new_plan);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		switch (error.Type()) {
		case ExceptionType::NOT_IMPLEMENTED: // NOLINT: explicitly allowing these errors (for now)
			break;                           // pass
		default:
			throw;
		}
	}
}

} // namespace duckdb
