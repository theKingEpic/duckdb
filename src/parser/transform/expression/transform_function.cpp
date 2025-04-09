#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

void Transformer::TransformWindowDef(duckdb_libpgquery::PGWindowDef &window_spec, WindowExpression &expr,
                                     const char *window_name) {
	// next: partitioning/ordering expressions
	if (window_spec.partitionClause) {
		if (window_name && !expr.partitions.empty()) {
			throw ParserException("Cannot override PARTITION BY clause of window \"%s\"", window_name);
		}
		TransformExpressionList(*window_spec.partitionClause, expr.partitions);
	}
	if (window_spec.orderClause) {
		if (window_name && !expr.orders.empty()) {
			throw ParserException("Cannot override ORDER BY clause of window \"%s\"", window_name);
		}
		TransformOrderBy(window_spec.orderClause, expr.orders);
		for (auto &order : expr.orders) {
			if (order.expression->GetExpressionType() == ExpressionType::STAR) {
				throw ParserException("Cannot ORDER BY ALL in a window expression");
			}
		}
	}
}

static inline WindowBoundary TransformFrameOption(const int frameOptions, const WindowBoundary rows,
                                                  const WindowBoundary range, const WindowBoundary groups) {

	if (frameOptions & FRAMEOPTION_RANGE) {
		return range;
	} else if (frameOptions & FRAMEOPTION_GROUPS) {
		return groups;
	} else {
		return rows;
	}
}

static bool IsExcludableWindowFunction(ExpressionType type) {
	switch (type) {
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
	case ExpressionType::WINDOW_NTH_VALUE:
	case ExpressionType::WINDOW_AGGREGATE:
		return true;
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_NTILE:
	case ExpressionType::WINDOW_CUME_DIST:
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		return false;
	default:
		throw InternalException("Unknown excludable window type %s", ExpressionTypeToString(type).c_str());
	}
}

void Transformer::TransformWindowFrame(duckdb_libpgquery::PGWindowDef &window_spec, WindowExpression &expr) {
	// finally: specifics of bounds
	expr.start_expr = TransformExpression(window_spec.startOffset);
	expr.end_expr = TransformExpression(window_spec.endOffset);

	if ((window_spec.frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING) ||
	    (window_spec.frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)) {
		throw InternalException(
		    "Window frames starting with unbounded following or ending in unbounded preceding make no sense");
	}

	if (window_spec.frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) {
		expr.start = WindowBoundary::UNBOUNDED_PRECEDING;
	} else if (window_spec.frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) {
		expr.start = TransformFrameOption(window_spec.frameOptions, WindowBoundary::EXPR_PRECEDING_ROWS,
		                                  WindowBoundary::EXPR_PRECEDING_RANGE, WindowBoundary::EXPR_PRECEDING_GROUPS);
	} else if (window_spec.frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING) {
		expr.start = TransformFrameOption(window_spec.frameOptions, WindowBoundary::EXPR_FOLLOWING_ROWS,
		                                  WindowBoundary::EXPR_FOLLOWING_RANGE, WindowBoundary::EXPR_FOLLOWING_GROUPS);
	} else if (window_spec.frameOptions & FRAMEOPTION_START_CURRENT_ROW) {
		expr.start = TransformFrameOption(window_spec.frameOptions, WindowBoundary::CURRENT_ROW_ROWS,
		                                  WindowBoundary::CURRENT_ROW_RANGE, WindowBoundary::CURRENT_ROW_GROUPS);
	}

	if (window_spec.frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
		expr.end = WindowBoundary::UNBOUNDED_FOLLOWING;
	} else if (window_spec.frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) {
		expr.end = TransformFrameOption(window_spec.frameOptions, WindowBoundary::EXPR_PRECEDING_ROWS,
		                                WindowBoundary::EXPR_PRECEDING_RANGE, WindowBoundary::EXPR_PRECEDING_GROUPS);
	} else if (window_spec.frameOptions & FRAMEOPTION_END_OFFSET_FOLLOWING) {
		expr.end = TransformFrameOption(window_spec.frameOptions, WindowBoundary::EXPR_FOLLOWING_ROWS,
		                                WindowBoundary::EXPR_FOLLOWING_RANGE, WindowBoundary::EXPR_FOLLOWING_GROUPS);
	} else if (window_spec.frameOptions & FRAMEOPTION_END_CURRENT_ROW) {
		expr.end = TransformFrameOption(window_spec.frameOptions, WindowBoundary::CURRENT_ROW_ROWS,
		                                WindowBoundary::CURRENT_ROW_RANGE, WindowBoundary::CURRENT_ROW_GROUPS);
	}

	D_ASSERT(expr.start != WindowBoundary::INVALID && expr.end != WindowBoundary::INVALID);
	if (((window_spec.frameOptions & (FRAMEOPTION_START_OFFSET_PRECEDING | FRAMEOPTION_START_OFFSET_FOLLOWING)) &&
	     !expr.start_expr) ||
	    ((window_spec.frameOptions & (FRAMEOPTION_END_OFFSET_PRECEDING | FRAMEOPTION_END_OFFSET_FOLLOWING)) &&
	     !expr.end_expr)) {
		throw InternalException("Failed to transform window boundary expression");
	}

	if (window_spec.frameOptions & FRAMEOPTION_EXCLUDE_CURRENT_ROW) {
		expr.exclude_clause = WindowExcludeMode::CURRENT_ROW;
	} else if (window_spec.frameOptions & FRAMEOPTION_EXCLUDE_GROUP) {
		expr.exclude_clause = WindowExcludeMode::GROUP;
	} else if (window_spec.frameOptions & FRAMEOPTION_EXCLUDE_TIES) {
		expr.exclude_clause = WindowExcludeMode::TIES;
	} else {
		expr.exclude_clause = WindowExcludeMode::NO_OTHER;
	}

	if (expr.exclude_clause != WindowExcludeMode::NO_OTHER && !expr.arg_orders.empty() &&
	    !IsExcludableWindowFunction(expr.type)) {
		throw ParserException("EXCLUDE is not supported for the window function \"%s\"", expr.function_name.c_str());
	}
}

bool Transformer::ExpressionIsEmptyStar(ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::STAR) {
		return false;
	}
	auto &star = expr.Cast<StarExpression>();
	if (!star.columns && star.exclude_list.empty() && star.replace_list.empty()) {
		return true;
	}
	return false;
}

bool Transformer::InWindowDefinition() {
	if (in_window_definition) {
		return true;
	}
	if (parent) {
		return parent->InWindowDefinition();
	}
	return false;
}

static bool IsOrderableWindowFunction(ExpressionType type) {
	switch (type) {
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
	case ExpressionType::WINDOW_NTH_VALUE:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_NTILE:
	case ExpressionType::WINDOW_CUME_DIST:
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
	case ExpressionType::WINDOW_AGGREGATE:
		return true;
	case ExpressionType::WINDOW_RANK_DENSE:
		return false;
	default:
		throw InternalException("Unknown orderable window type %s", ExpressionTypeToString(type).c_str());
	}
}

// 将 PostgreSQL 的函数调用节点转换为 DuckDB 的表达式
unique_ptr<ParsedExpression> Transformer::TransformFuncCall(duckdb_libpgquery::PGFuncCall &root) {
	// 获取函数名链表指针
	auto name = root.funcname;

	// 初始化三级命名结构：catalog.schema.function_name
	string catalog, schema, function_name;

	// 解析三级限定名（如 pg_catalog.substr）
	if (name->length == 3) {
		catalog = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->data.ptr_value)->val.str; // 解析 catalog
		schema = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->next->data.ptr_value)->val.str; // 解析 schema
		function_name = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->next->next->data.ptr_value)->val.str; // 解析函数名
	}
	// 解析二级限定名（如 public.myfunc）
	else if (name->length == 2) {
		catalog = INVALID_CATALOG; // 无效 catalog 标识
		schema = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->data.ptr_value)->val.str; // 解析 schema
		function_name = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->next->data.ptr_value)->val.str; // 解析函数名
	}
	// 解析单级函数名（如 sum）
	else if (name->length == 1) {
		catalog = INVALID_CATALOG;
		schema = INVALID_SCHEMA;
		function_name = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->data.ptr_value)->val.str; // 直接获取函数名
	}
	// 非法限定名长度处理
	else {
		throw ParserException("TransformFuncCall - Expected 1, 2 or 3 qualifications"); // 抛出语法错误
	}

	// 转换函数参数列表 -------------------------------------------------
	vector<unique_ptr<ParsedExpression>> children;
	if (root.args) {
		TransformExpressionList(*root.args, children); // 递归转换所有参数表达式
	}

	// 特殊处理 COUNT(*) 语法（转换为无参数形式）
	if (children.size() == 1 && ExpressionIsEmptyStar(*children[0]) && !root.agg_distinct && !root.agg_order) {
		// COUNT(*) gets translated into COUNT()
		children.clear();// 清空参数列表（COUNT(*) -> COUNT()）
	}

	// 窗口函数处理分支 -------------------------------------------------
	auto lowercase_name = StringUtil::Lower(function_name); // 统一小写处理
	if (root.over) { // 检测 OVER 子句存在
		// 禁止在窗口定义中嵌套窗口函数（如 RANK() OVER (RANK() OVER...))
		if (InWindowDefinition()) {
			throw ParserException("window functions are not allowed in window definitions");
		}

		// 将函数名映射为窗口表达式类型（如 RANK -> WINDOW_RANK）
		const auto win_fun_type = WindowExpression::WindowToExpressionType(lowercase_name);
		if (win_fun_type == ExpressionType::INVALID) {
			throw InternalException("Unknown/unsupported window function"); // 内部错误：未知窗口函数
		}

		// 校验非聚合窗口函数不允许 DISTINCT（如 RANK() DISTINCT）
		if (win_fun_type != ExpressionType::WINDOW_AGGREGATE && root.agg_distinct) {
			throw ParserException("DISTINCT is not implemented for non-aggregate window functions!");
		}

		// 校验不可排序的窗口函数不允许 ORDER BY（如 ROW_NUMBER() ORDER BY）
		if (root.agg_order && !IsOrderableWindowFunction(win_fun_type)) {
			throw ParserException("ORDER BY is not supported for the window function \"%s\"", lowercase_name.c_str());
		}

		// 校验非聚合窗口函数不允许 FILTER（如 LEAD(...) FILTER ...）
		if (win_fun_type != ExpressionType::WINDOW_AGGREGATE && root.agg_filter) {
			throw ParserException("FILTER is not implemented for non-aggregate window functions!");
		}

		// 窗口函数不支持 EXPORT_STATE 修饰符
		if (root.export_state) {
			throw ParserException("EXPORT_STATE is not supported for window functions!");
		}

		// 校验聚合窗口函数不允许 RESPECT/IGNORE NULLS（如 SUM(...) IGNORE NULLS）
		if (win_fun_type == ExpressionType::WINDOW_AGGREGATE &&
			root.agg_ignore_nulls != duckdb_libpgquery::PG_DEFAULT_NULLS) {
			throw ParserException("RESPECT/IGNORE NULLS is not supported for windowed aggregates");}

		// 创建窗口表达式节点
		auto expr = make_uniq<WindowExpression>(win_fun_type, std::move(catalog), std::move(schema), lowercase_name);
		expr->ignore_nulls = (root.agg_ignore_nulls == duckdb_libpgquery::PG_IGNORE_NULLS); // 设置空值处理标记
		expr->distinct = root.agg_distinct; // 设置 DISTINCT 标记

		// 转换 FILTER 子句（如 SUM(x) FILTER (WHERE x > 0))
		if (root.agg_filter) {
			auto filter_expr = TransformExpression(root.agg_filter); // 递归转换过滤条件
			expr->filter_expr = std::move(filter_expr); // 挂载到窗口表达式
		}

		// 转换 ORDER BY 子句（窗口函数局部排序）
		if (root.agg_order) {
			auto order_bys = make_uniq<OrderModifier>(); // 创建排序修饰符
			TransformOrderBy(root.agg_order, order_bys->orders); // 转换排序表达式列表
			expr->arg_orders = std::move(order_bys->orders); // 挂载排序表达式
		}

		// 处理不同类型的窗口函数参数
		if (win_fun_type == ExpressionType::WINDOW_AGGREGATE) {
			// 聚合窗口函数（如 SUM/COUNT）直接继承所有参数
			expr->children = std::move(children);
		} else {
			// 非聚合窗口函数最多一个主参数
			if (!children.empty()) {
				expr->children.push_back(std::move(children[0]));// 第一个参数作为主参数
			}
			// LEAD/LAG 特殊参数处理（offset, default）
			if (win_fun_type == ExpressionType::WINDOW_LEAD || win_fun_type == ExpressionType::WINDOW_LAG) {
				if (children.size() > 1) {
					expr->offset_expr = std::move(children[1]);// 第二个参数为偏移量
				}
				if (children.size() > 2) {
					expr->default_expr = std::move(children[2]);// 第三个参数为默认值
				}
				if (children.size() > 3) {// 参数超限报错
					throw ParserException("Incorrect number of parameters for function %s", lowercase_name);
				}
			}
			// NTH_VALUE 参数处理（n值）
			else if (win_fun_type == ExpressionType::WINDOW_NTH_VALUE) {
				if (children.size() > 1) {
					expr->children.push_back(std::move(children[1]));// 第二个参数为n值
				}
				if (children.size() > 2) {// 参数超限报错
					throw ParserException("Incorrect number of parameters for function %s", lowercase_name);
				}
			} else {// 其他窗口函数参数校验（如 RANK 不允许参数）
				if (children.size() > 1) {
					throw ParserException("Incorrect number of parameters for function %s", lowercase_name);
				}
			}
		}
		// 解析窗口定义（PARTITION BY/ORDER BY/FRAME子句）
		auto window_spec = PGPointerCast<duckdb_libpgquery::PGWindowDef>(root.over); // 获取窗口规格
		// 处理命名窗口引用（WINDOW win AS (...))
		if (window_spec->name) {
			auto it = window_clauses.find(string(window_spec->name)); // 查找预定义窗口
			if (it == window_clauses.end()) {
				throw ParserException("window \"%s\" does not exist", window_spec->name); // 未找到定义报错
			}
			window_spec = it->second; // 使用已定义的窗口规格
			D_ASSERT(window_spec); // 断言有效性
		}
		// 处理窗口引用链（如 OVER win1，其中 win1 引用了 win2
		auto window_ref = window_spec;
		auto window_name = window_ref->refname;
		if (window_ref->refname) { // 存在引用其他窗口
			auto it = window_clauses.find(string(window_spec->refname)); // 查找被引用窗口
			if (it == window_clauses.end()) {
				throw ParserException("window \"%s\" does not exist", window_spec->refname);
			}
			window_ref = it->second; // 获取被引用窗口规格
			D_ASSERT(window_ref);
			// 校验被引用窗口不能包含帧定义（如 ROWS BETWEEN...）
			if (window_ref->startOffset || window_ref->endOffset || window_ref->frameOptions != FRAMEOPTION_DEFAULTS) {
				throw ParserException("cannot copy window \"%s\" because it has a frame clause", window_spec->refname);
			}
		}
		// 转换窗口定义
		in_window_definition = true; // 设置窗口解析标志
		TransformWindowDef(*window_ref, *expr); // 转换基础定义（PARTITION BY/ORDER BY）
		if (window_ref != window_spec) { // 处理引用链合并
			TransformWindowDef(*window_spec, *expr, window_name); // 合并当前窗口规格
		}
		TransformWindowFrame(*window_spec, *expr); // 转换窗口帧定义（ROWS/RANGE）
		in_window_definition = false; // 清除窗口解析标志
		SetQueryLocation(*expr, root.location); // 记录源码位置信息
		return std::move(expr); // 返回窗口表达式
	}
	// 非窗口函数处理 -------------------------------------------------

	// 校验普通函数不支持 RESPECT/IGNORE NULLS
	if (root.agg_ignore_nulls != duckdb_libpgquery::PG_DEFAULT_NULLS) {
		throw ParserException("RESPECT/IGNORE NULLS is not supported for non-window functions");
	}

	// 转换 FILTER 子句（普通聚合函数）
	unique_ptr<ParsedExpression> filter_expr;
	if (root.agg_filter) {
		filter_expr = TransformExpression(root.agg_filter);
	}

	// 转换 ORDER BY 子句（普通聚合函数）
	auto order_bys = make_uniq<OrderModifier>();
	if (root.agg_order) {
		TransformOrderBy(root.agg_order, order_bys->orders);
	}

	// 处理 WITHIN GROUP 语法（PostgreSQL 有序聚合）
	// Ordered aggregates can be either WITHIN GROUP or after the function arguments
	if (root.agg_within_group) {
		// 校验 ORDER BY 数量（WITHIN GROUP 仅允许单列排序）
		//	https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-ORDEREDSET-TABLE
		//  Since we implement "ordered aggregates" without sorting,
		//  we map all the ones we support to the corresponding aggregate function.
		if (order_bys->orders.size() != 1) {
			throw ParserException("Cannot use multiple ORDER BY clauses with WITHIN GROUP");
		}
		// 重写函数名映射（兼容 PostgreSQL 语法）
		if (lowercase_name == "percentile_cont") {//计算连续百分位数 必须且只能有 1 个参数（百分位值）
			if (children.size() != 1) {
				throw ParserException("Wrong number of arguments for PERCENTILE_CONT");
			}
			lowercase_name = "quantile_cont";// 映射到 DuckDB 内部函数
		} else if (lowercase_name == "percentile_disc") {//计算离散百分位数。
			if (children.size() != 1) {
				throw ParserException("Wrong number of arguments for PERCENTILE_DISC");
			}
			lowercase_name = "quantile_disc";
		} else if (lowercase_name == "mode") {//计算众数（出现频率最高的值） 必须无参数
			if (!children.empty()) {
				throw ParserException("Wrong number of arguments for MODE");
			}
			lowercase_name = "mode";
		} else {//未知函数
			throw ParserException("Unknown ordered aggregate \"%s\".", function_name);
		}
	}

	// star gets eaten in the parser 处理 COUNT(*) 语法（参数已清空时重命名）
	if (lowercase_name == "count" && children.empty()) {
		lowercase_name = "count_star"; // 转换为内部 COUNT_STAR 函数
	}
	// 特殊函数重写逻辑 -------------------------------------------------

	// IF 函数转 CASE 表达式（IF(cond, a, b) -> CASE WHEN cond THEN a ELSE b
	if (lowercase_name == "if") {
		if (children.size() != 3) {
			throw ParserException("Wrong number of arguments to IF.");
		}
		auto expr = make_uniq<CaseExpression>();
		CaseCheck check;
		check.when_expr = std::move(children[0]);// 条件表达式
		check.then_expr = std::move(children[1]);// THEN 表达式
		expr->case_checks.push_back(std::move(check));
		expr->else_expr = std::move(children[2]);// ELSE 表达式
		return std::move(expr);
	} else if (lowercase_name == "try") {
		if (children.size() != 1) {
			throw ParserException("Wrong number of arguments provided to TRY expression");
		}
		auto try_expression = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_TRY);
		try_expression->children = std::move(children);
		return std::move(try_expression);
	} else if (lowercase_name == "construct_array") {// 数组构造器语法转换（construct_array(a,b,c) -> ARRAY[a,b,c]）
		auto construct_array = make_uniq<OperatorExpression>(ExpressionType::ARRAY_CONSTRUCTOR);
		construct_array->children = std::move(children);// 继承所有参数
		return std::move(construct_array);
	} else if (lowercase_name == "__internal_position_operator") {// 内部位置操作符转换（POSITION(x IN y) 参数顺序调整）
		if (children.size() != 2) {
			throw ParserException("Wrong number of arguments to __internal_position_operator.");
		}
		// swap arguments for POSITION(x IN y)
		std::swap(children[0], children[1]);// 交换参数顺序（y, x -> x, y）
		lowercase_name = "position";// 重命名为标准函数名
	} // 检查函数名是否为 "list" 且 ORDER BY 子句有且仅有一个排序规则
	else if (lowercase_name == "list" && order_bys->orders.size() == 1) {
		// 验证参数数量必须为 1（LIST 只接受一个聚合表达式）
		if (children.size() != 1) {
			throw ParserException("Wrong number of arguments to LIST.");
		}

		// 提取第一个参数表达式（例如 name）
		auto arg_expr = children[0].get();
		// 获取 ORDER BY 的排序规则（例如 ORDER BY age DESC NULLS FIRST）
		auto &order_by = order_bys->orders[0];

		// 检查聚合表达式是否与 ORDER BY 表达式一致（例如 name vs age）
		if (arg_expr->Equals(*order_by.expression)) {
			// 将排序方向（ASC/DESC）转为常量表达式（例如 'DESC'）
			auto sense = make_uniq<ConstantExpression>(EnumUtil::ToChars(order_by.type));
			// 将空值处理规则转为常量表达式（例如 'NULLS FIRST'）
			auto nulls = make_uniq<ConstantExpression>(EnumUtil::ToChars(order_by.null_order));
			order_bys = nullptr; // 清空原 ORDER BY 信息

			// 生成未排序的 LIST 表达式（例如 LIST(name)）
			auto unordered = make_uniq<FunctionExpression>(catalog, schema, lowercase_name.c_str(),
														 std::move(children), std::move(filter_expr),
														 std::move(order_bys), root.agg_distinct,
														 false, root.export_state);

			// 将函数名改为 list_sort（例如 LIST_SORT(...)）
			lowercase_name = "list_sort";
			order_bys.reset();   // 重置 ORDER BY
			filter_expr.reset(); // 重置过滤条件
			children.clear();    // 清空子节点
			root.agg_distinct = false; // 禁用 DISTINCT

			// 构建新的参数列表：
			// 1. 未排序的 LIST 结果（例如 LIST(name)）
			// 2. 排序方向（例如 'DESC'）
			// 3. 空值处理规则（例如 'NULLS FIRST'）
			children.emplace_back(std::move(unordered));
			children.emplace_back(std::move(sense));
			children.emplace_back(std::move(nulls));
		}
	}


	// 构建标准函数表达式 -------------------------------------------------
	auto function = make_uniq<FunctionExpression>(
		std::move(catalog), std::move(schema), lowercase_name.c_str(),
		std::move(children), // 参数列表
		std::move(filter_expr), // FILTER 表达式
		std::move(order_bys), // ORDER BY 列表
		root.agg_distinct, // DISTINCT 标记
		false, // 是否为导出状态（未使用）
		root.export_state // EXPORT_STATE 标记
	);
	SetQueryLocation(*function, root.location); // 记录源码位置
	return std::move(function); // 返回最终表达式
}

unique_ptr<ParsedExpression> Transformer::TransformSQLValueFunction(duckdb_libpgquery::PGSQLValueFunction &node) {
	throw InternalException("SQL value functions should not be emitted by the parser");
}

} // namespace duckdb
