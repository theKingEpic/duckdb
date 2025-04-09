#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<QueryNode> Transformer::TransformSelectNode(duckdb_libpgquery::PGNode &node, bool is_select) {
	switch (node.type) {
	case duckdb_libpgquery::T_PGVariableShowSelectStmt:
		return TransformShowSelect(PGCast<duckdb_libpgquery::PGVariableShowSelectStmt>(node));
	case duckdb_libpgquery::T_PGVariableShowStmt:
		return TransformShow(PGCast<duckdb_libpgquery::PGVariableShowStmt>(node));
	default:
		return TransformSelectNodeInternal(PGCast<duckdb_libpgquery::PGSelectStmt>(node), is_select);
	}
}

// 转换SELECT语句的核心函数，处理常规SELECT或PIVOT语句，返回查询树节点
unique_ptr<QueryNode> Transformer::TransformSelectNodeInternal(
	duckdb_libpgquery::PGSelectStmt &select,  // 输入的PostgreSQL解析树SELECT节点
	bool is_select                            // 标记是否为纯SELECT语句（非INSERT/CREATE TABLE AS）
) {
	// 针对纯SELECT语句的额外语法校验
	if (is_select) {
		// 拦截SELECT INTO语法（DuckDB不支持该语法，需用CREATE TABLE AS替代）
		if (select.intoClause) {
			throw ParserException("SELECT INTO not supported!");
		}
		// 拦截行锁语法（如FOR UPDATE，DuckDB未实现）
		if (select.lockingClause) {
			throw ParserException("SELECT locking clause is not supported!");
		}
	}

	unique_ptr<QueryNode> stmt = nullptr;  // 最终生成的查询树节点

	// 分支处理PIVOT语法和常规SELECT
	if (select.pivot) {
		// 调用PIVOT专用转换器（生成包含Pivot操作的查询树）
		stmt = TransformPivotStatement(select);
	} else {
		// 常规SELECT转换（处理投影、JOIN、WHERE等子句）
		stmt = TransformSelectInternal(select);
	}

	// 处理WITH子句的物化（将CTE转换为临时数据结构）
	return TransformMaterializedCTE(std::move(stmt));
}

unique_ptr<SelectStatement> Transformer::TransformSelectStmt(duckdb_libpgquery::PGSelectStmt &select, bool is_select) {
	auto result = make_uniq<SelectStatement>();
	result->node = TransformSelectNodeInternal(select, is_select);
	return result;
}

unique_ptr<SelectStatement> Transformer::TransformSelectStmt(duckdb_libpgquery::PGNode &node, bool is_select) {
	auto select_node = TransformSelectNode(node, is_select);
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	return select_statement;
}

} // namespace duckdb
