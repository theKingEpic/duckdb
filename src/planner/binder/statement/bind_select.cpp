#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

// 绑定SELECT语句的入口函数，将SQL语句转换为可执行的BoundStatement
BoundStatement Binder::Bind(SelectStatement &stmt) {
	// 获取当前语句的属性对象引用，用于设置和传递语句级属性
	auto &properties = GetStatementProperties();

	// 设置允许流式结果属性为true，表示该查询结果可以流式返回
	// 流式结果适用于大数据集，避免一次性加载所有数据到内存
	properties.allow_stream_result = true;

	// 设置返回类型为查询结果，区别于其他语句类型如CREATE/INSERT等
	// QUERY_RESULT类型表示该语句会产生结果集
	properties.return_type = StatementReturnType::QUERY_RESULT;

	// 将实际绑定工作委托给Bind(QueryNode&)方法
	// stmt.node包含SELECT查询的抽象语法树(AST)节点
	// 通过解引用后传递给下层绑定方法
	return Bind(*stmt.node);
}

} // namespace duckdb
