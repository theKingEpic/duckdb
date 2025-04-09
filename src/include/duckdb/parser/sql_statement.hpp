//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/sql_statement.hpp
//
// SQL语句的基类定义，所有类型的SQL语句都继承自此类
//
//===----------------------------------------------------------------------===//

#pragma once  // 头文件保护，防止重复包含

// 引入必要的头文件
#include "duckdb/common/common.hpp"               // 通用工具和类型
#include "duckdb/common/enums/statement_type.hpp" // 语句类型枚举
#include "duckdb/common/exception.hpp"            // 异常处理
#include "duckdb/common/printer.hpp"              // 打印工具
#include "duckdb/common/named_parameter_map.hpp"  // 命名参数映射容器

namespace duckdb {

//! SQLStatement是所有SQL语句类型的基类
class SQLStatement {
public:
    // 静态常量，表示无效的语句类型（派生类需要覆盖）
    static constexpr const StatementType TYPE = StatementType::INVALID_STATEMENT;

public:
    // 显式构造函数，需要指定语句类型
    explicit SQLStatement(StatementType type) : type(type) {}

    // 虚析构函数，确保正确释放派生类对象
    virtual ~SQLStatement() {}

    // 成员变量
    //! 语句类型标识
    StatementType type;

    //! 语句在查询字符串中的起始位置（字节偏移）
    idx_t stmt_location = 0;

    //! 语句在查询字符串中的长度（字节数）
    idx_t stmt_length = 0;

    //! 命名参数映射表（参数名 -> 参数索引）
    case_insensitive_map_t<idx_t> named_param_map;

    //! 对应的原始查询文本
    string query;

protected:
    // 保护拷贝构造函数（派生类拷贝时需要调用基类的拷贝构造）
    SQLStatement(const SQLStatement &other) = default;

public:
    // 纯虚函数：将语句转换为字符串表示
    virtual string ToString() const = 0;

    // 纯虚函数：创建语句的深拷贝副本
    DUCKDB_API virtual unique_ptr<SQLStatement> Copy() const = 0;

public:
    // 类型安全转换模板方法
    template <class TARGET>
    TARGET &Cast() {
        // 类型安全检查：目标类型必须匹配当前语句类型
        if (type != TARGET::TYPE && TARGET::TYPE != StatementType::INVALID_STATEMENT) {
            throw InternalException("Failed to cast statement to type - statement type mismatch");
        }
        return reinterpret_cast<TARGET &>(*this);  // 执行转换
    }

    // const版本的类型安全转换
    template <class TARGET>
    const TARGET &Cast() const {
        if (type != TARGET::TYPE && TARGET::TYPE != StatementType::INVALID_STATEMENT) {
            throw InternalException("Failed to cast statement to type - statement type mismatch");
        }
        return reinterpret_cast<const TARGET &>(*this);
    }
};

} // namespace duckdb