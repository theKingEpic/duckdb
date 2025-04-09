#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"

namespace duckdb {

void Transformer::TransformModifiers(duckdb_libpgquery::PGSelectStmt &stmt, QueryNode &node) {
	// transform the common properties
	// both the set operations and the regular select can have an ORDER BY/LIMIT attached to them
	vector<OrderByNode> orders;
	TransformOrderBy(stmt.sortClause, orders);
	if (!orders.empty()) {
		auto order_modifier = make_uniq<OrderModifier>();
		order_modifier->orders = std::move(orders);
		node.modifiers.push_back(std::move(order_modifier));
	}

	if (stmt.limitCount || stmt.limitOffset) {
		if (stmt.limitCount && stmt.limitCount->type == duckdb_libpgquery::T_PGLimitPercent) {
			auto limit_percent_modifier = make_uniq<LimitPercentModifier>();
			auto expr_node = PGPointerCast<duckdb_libpgquery::PGLimitPercent>(stmt.limitCount)->limit_percent;
			limit_percent_modifier->limit = TransformExpression(expr_node);
			if (stmt.limitOffset) {
				limit_percent_modifier->offset = TransformExpression(stmt.limitOffset);
			}
			node.modifiers.push_back(std::move(limit_percent_modifier));
		} else {
			auto limit_modifier = make_uniq<LimitModifier>();
			if (stmt.offset_first) {
				if (stmt.limitOffset) {
					limit_modifier->offset = TransformExpression(stmt.limitOffset);
				}
				if (stmt.limitCount) {
					limit_modifier->limit = TransformExpression(stmt.limitCount);
				}
			} else {
				if (stmt.limitCount) {
					limit_modifier->limit = TransformExpression(stmt.limitCount);
				}
				if (stmt.limitOffset) {
					limit_modifier->offset = TransformExpression(stmt.limitOffset);
				}
			}
			node.modifiers.push_back(std::move(limit_modifier));
		}
	}
}

// 将 PostgreSQL 解析树中的 SELECT 语句转换为 DuckDB 内部的查询节点树
unique_ptr<QueryNode> Transformer::TransformSelectInternal(duckdb_libpgquery::PGSelectStmt &stmt) {
    // 断言验证输入节点类型正确（必须为 SELECT 语句节点）
    D_ASSERT(stmt.type == duckdb_libpgquery::T_PGSelectStmt);
    // 栈检查器，防止递归过深导致栈溢出（常见于复杂嵌套查询）
    auto stack_checker = StackCheck();

    unique_ptr<QueryNode> node;  // 最终生成的查询树根节点

    // 根据 SELECT 的操作类型分支处理（普通查询或集合操作）
    switch (stmt.op) {
    // 处理普通 SELECT 查询（非 UNION/EXCEPT 等）
    case duckdb_libpgquery::PG_SETOP_NONE: {
        node = make_uniq<SelectNode>();  // 创建 Select 查询节点
        auto &result = node->Cast<SelectNode>();

        // 转换 WITH CTE 子句（将 CTE 存入当前节点的 cte_map）
        if (stmt.withClause) {
            TransformCTE(*PGPointerCast<duckdb_libpgquery::PGWithClause>(stmt.withClause), node->cte_map);
        }

        // 处理 WINDOW 窗口函数定义（解析窗口定义并检查重复）
        if (stmt.windowClause) {
            for (auto window_ele = stmt.windowClause->head; window_ele != nullptr; window_ele = window_ele->next) {
                auto window_def = PGPointerCast<duckdb_libpgquery::PGWindowDef>(window_ele->data.ptr_value);
                D_ASSERT(window_def && window_def->name);
                string window_name(window_def->name);
                // 检查窗口名是否重复定义
                if (window_clauses.find(window_name) != window_clauses.end()) {
                    throw ParserException("window \"%s\" is already defined", window_name);
                }
                window_clauses[window_name] = window_def.get();  // 暂存窗口定义供后续使用
            }
        }

        // 处理 DISTINCT 子句
        if (stmt.distinctClause != nullptr) {
            auto modifier = make_uniq<DistinctModifier>();  // 创建 DISTINCT 修饰符
            // 处理 DISTINCT ON (columns) 语法（PostgreSQL 特有）
            auto target = PGPointerCast<duckdb_libpgquery::PGNode>(stmt.distinctClause->head->data.ptr_value);
            if (target) {
                // 将 DISTINCT ON 的列转换为表达式列表
                TransformExpressionList(*stmt.distinctClause, modifier->distinct_on_targets);
            }
            result.modifiers.push_back(std::move(modifier));  // 添加修饰符到查询节点
        }

        // 处理 VALUES 列表（特殊场景，如 SELECT * FROM (VALUES ...)）
        if (stmt.valuesLists) {
            // VALUES 列表生成虚拟表（无需 FROM 子句）
            D_ASSERT(!stmt.fromClause);
            result.from_table = TransformValuesList(stmt.valuesLists);  // 转换 VALUES 为表达式列表
            result.select_list.push_back(make_uniq<StarExpression>());  // 添加星号表达式表示所有列
        } else {
            // 常规 SELECT 处理（必须有目标列）
            if (!stmt.targetList) {
                throw ParserException("SELECT clause without selection list");
            }
            // 根据 from_first 标志决定先转换 FROM 还是 SELECT 列表（影响参数位置解析）
            if (stmt.from_first) {
                result.from_table = TransformFrom(stmt.fromClause);  // 转换 FROM 子句为 Join 树
                TransformExpressionList(*stmt.targetList, result.select_list);  // 转换选择列
            } else {
                TransformExpressionList(*stmt.targetList, result.select_list);
                result.from_table = TransformFrom(stmt.fromClause);
            }
        }

        // 转换 WHERE 条件
        result.where_clause = TransformExpression(stmt.whereClause);
        // 转换 GROUP BY 子句（包括处理 ROLLUP/CUBE 等）
        TransformGroupBy(stmt.groupClause, result);
        // 转换 HAVING 条件
        result.having = TransformExpression(stmt.havingClause);
        // 转换 QUALIFY 子句（过滤窗口函数结果）
        result.qualify = TransformExpression(stmt.qualifyClause);
        // 转换 SAMPLE 抽样子句（如 TABLESAMPLE BERNOULLI(50)）
        result.sample = TransformSampleOptions(stmt.sampleOptions);
        break;
    }

    // 处理集合操作（UNION/EXCEPT/INTERSECT/UNION BY NAME）
    case duckdb_libpgquery::PG_SETOP_UNION:
    case duckdb_libpgquery::PG_SETOP_EXCEPT:
    case duckdb_libpgquery::PG_SETOP_INTERSECT:
    case duckdb_libpgquery::PG_SETOP_UNION_BY_NAME: {
        node = make_uniq<SetOperationNode>();  // 创建集合操作节点
        auto &result = node->Cast<SetOperationNode>();

        // 处理 WITH CTE（与普通 SELECT 类似）
        if (stmt.withClause) {
            TransformCTE(*PGPointerCast<duckdb_libpgquery::PGWithClause>(stmt.withClause), node->cte_map);
        }

        // 递归转换左右子查询（如 UNION 两侧的 SELECT）
        result.left = TransformSelectNode(*stmt.larg);
        result.right = TransformSelectNode(*stmt.rarg);
        if (!result.left || !result.right) {
            throw InternalException("Failed to transform setop children.");
        }

        // 设置集合操作属性
        result.setop_all = stmt.all;  // 是否保留重复行（ALL 修饰符）
        switch (stmt.op) {           // 设置操作类型
            case duckdb_libpgquery::PG_SETOP_UNION:
                result.setop_type = SetOperationType::UNION;
                break;
            case duckdb_libpgquery::PG_SETOP_EXCEPT:
                result.setop_type = SetOperationType::EXCEPT;
                break;
            case duckdb_libpgquery::PG_SETOP_INTERSECT:
                result.setop_type = SetOperationType::INTERSECT;
                break;
            case duckdb_libpgquery::PG_SETOP_UNION_BY_NAME:
                result.setop_type = SetOperationType::UNION_BY_NAME;  // 按列名而非位置合并
                break;
            default:
                throw InternalException("Unexpected setop type");
        }

        // 集合操作不支持 SAMPLE 子句
        if (stmt.sampleOptions) {
            throw ParserException("SAMPLE clause is only allowed in regular SELECT statements");
        }
        break;
    }
    default:
        throw NotImplementedException("Statement type %d not implemented!", stmt.op);
    }

    // 统一处理排序、限制等修饰符（ORDER BY/LIMIT/OFFSET）
    TransformModifiers(stmt, *node);

    return node;  // 返回构建完成的查询节点树
}

} // namespace duckdb
