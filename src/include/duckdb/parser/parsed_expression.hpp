//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/base_expression.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/expression_util.hpp"

namespace duckdb {
class Deserializer;
class Serializer;

//!  The ParsedExpression class is a base class that can represent any expression
//!  part of a SQL statement.
/*!
 The ParsedExpression class is a base class that can represent any expression
 part of a SQL statement. This is, for example, a column reference in a SELECT
 clause, but also operators, aggregates or filters. The Expression is emitted by the parser and does not contain any
 information about bindings to the catalog or to the types. ParsedExpressions are transformed into regular Expressions
 in the Binder.
ParsedExpression类是一个基类，它可以表示SQL语句的任何表达式部分。例如，这是SELECT子句中的列引用，也可以是操作符、聚合或过滤器。表达式由解析器发出，不包含任何关于到编目或类型绑定的信息。在绑定器中将ParsedExpressions转换为正则表达式。
 */
// ParsedExpression 是表示 SQL 语句中任何表达式的基类
// 继承自 BaseExpression，提供表达式类型、分类等基础属性
class ParsedExpression : public BaseExpression {
public:
	// 构造函数：初始化表达式类型和分类（通过基类构造函数）
	ParsedExpression(ExpressionType type, ExpressionClass expression_class)
	    : BaseExpression(type, expression_class) {} // 委托基类构造

public:
	// 以下虚函数需要子类实现具体逻辑 ----
	bool IsAggregate() const override;    // 判断是否为聚合表达式（如 SUM()）
	bool IsWindow() const override;       // 判断是否为窗口函数（如 ROW_NUMBER()）
	bool HasSubquery() const override;    // 判断是否包含子查询
	bool IsScalar() const override;      // 判断是否为标量表达式（返回单一值）
	bool HasParameter() const override;   // 判断是否含参数占位符（如 $1）

	// 表达式等价性检查（需比较内部结构）
	bool Equals(const BaseExpression &other) const override;
	// 计算表达式的哈希值（用于哈希表存储）
	hash_t Hash() const override;

	// 克隆表达式对象的抽象方法（子类必须实现深拷贝）
	virtual unique_ptr<ParsedExpression> Copy() const = 0;

	// 序列化/反序列化方法（用于网络传输或持久化）
	virtual void Serialize(Serializer &serializer) const;      // 序列化到二进制流
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer); // 从流反序列化

	// 静态工具方法 ----
	// 比较两个表达式指针是否指向等价对象
	static bool Equals(const unique_ptr<ParsedExpression> &left,
			  const unique_ptr<ParsedExpression> &right);
	// 比较两个表达式指针列表是否等价
	static bool ListEquals(const vector<unique_ptr<ParsedExpression>> &left,
			      const vector<unique_ptr<ParsedExpression>> &right);

protected:
	// 拷贝基类属性到当前对象（用于子类实现 Copy 方法时调用）
	void CopyProperties(const ParsedExpression &other) {
		type = other.type;                // 表达式类型（如算术、逻辑）
		expression_class = other.expression_class; // 表达式分类（如聚合、窗口）
		alias = other.alias;               // 表达式的别名（AS 子句）
		query_location = other.query_location; // 在查询文本中的位置（用于错误提示）
	}
};

} // namespace duckdb
