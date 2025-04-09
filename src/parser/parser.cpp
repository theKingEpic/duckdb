#include "duckdb/parser/parser.hpp"

#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "parser/parser.hpp"
#include "postgres_parser.hpp"

namespace duckdb {

Parser::Parser(ParserOptions options_p) : options(options_p) {
}

struct UnicodeSpace {
	UnicodeSpace(idx_t pos, idx_t bytes) : pos(pos), bytes(bytes) {
	}

	idx_t pos;
	idx_t bytes;
};

static bool ReplaceUnicodeSpaces(const string &query, string &new_query, vector<UnicodeSpace> &unicode_spaces) {
	if (unicode_spaces.empty()) {
		// no unicode spaces found
		return false;
	}
	idx_t prev = 0;
	for (auto &usp : unicode_spaces) {
		new_query += query.substr(prev, usp.pos - prev);
		new_query += " ";
		prev = usp.pos + usp.bytes;
	}
	new_query += query.substr(prev, query.size() - prev);
	return true;
}

static bool IsValidDollarQuotedStringTagFirstChar(const unsigned char &c) {
	// the first character can be between A-Z, a-z, or \200 - \377
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c >= 0x80;
}

static bool IsValidDollarQuotedStringTagSubsequentChar(const unsigned char &c) {
	// subsequent characters can also be between 0-9
	return IsValidDollarQuotedStringTagFirstChar(c) || (c >= '0' && c <= '9');
}

// This function strips unicode space characters from the query and replaces them with regular spaces
// It returns true if any unicode space characters were found and stripped
// See here for a list of unicode space characters - https://jkorpela.fi/chars/spaces.html
bool Parser::StripUnicodeSpaces(const string &query_str, string &new_query) {
	const idx_t NBSP_LEN = 2;
	const idx_t USP_LEN = 3;
	idx_t pos = 0;
	unsigned char quote;
	string_t dollar_quote_tag;
	vector<UnicodeSpace> unicode_spaces;
	auto query = const_uchar_ptr_cast(query_str.c_str());
	auto qsize = query_str.size();

regular:
	for (; pos + 2 < qsize; pos++) {
		if (query[pos] == 0xC2) {
			if (query[pos + 1] == 0xA0) {
				// U+00A0 - C2A0
				unicode_spaces.emplace_back(pos, NBSP_LEN);
			}
		}
		if (query[pos] == 0xE2) {
			if (query[pos + 1] == 0x80) {
				if (query[pos + 2] >= 0x80 && query[pos + 2] <= 0x8B) {
					// U+2000 to U+200B
					// E28080 - E2808B
					unicode_spaces.emplace_back(pos, USP_LEN);
				} else if (query[pos + 2] == 0xAF) {
					// U+202F - E280AF
					unicode_spaces.emplace_back(pos, USP_LEN);
				}
			} else if (query[pos + 1] == 0x81) {
				if (query[pos + 2] == 0x9F) {
					// U+205F - E2819f
					unicode_spaces.emplace_back(pos, USP_LEN);
				} else if (query[pos + 2] == 0xA0) {
					// U+2060 - E281A0
					unicode_spaces.emplace_back(pos, USP_LEN);
				}
			}
		} else if (query[pos] == 0xE3) {
			if (query[pos + 1] == 0x80 && query[pos + 2] == 0x80) {
				// U+3000 - E38080
				unicode_spaces.emplace_back(pos, USP_LEN);
			}
		} else if (query[pos] == 0xEF) {
			if (query[pos + 1] == 0xBB && query[pos + 2] == 0xBF) {
				// U+FEFF - EFBBBF
				unicode_spaces.emplace_back(pos, USP_LEN);
			}
		} else if (query[pos] == '"' || query[pos] == '\'') {
			quote = query[pos];
			pos++;
			goto in_quotes;
		} else if (query[pos] == '$' &&
		           (query[pos + 1] == '$' || IsValidDollarQuotedStringTagFirstChar(query[pos + 1]))) {
			// (optionally tagged) dollar-quoted string
			auto start = &query[++pos];
			for (; pos + 2 < qsize; pos++) {
				if (query[pos] == '$') {
					// end of tag
					dollar_quote_tag =
					    string_t(const_char_ptr_cast(start), NumericCast<uint32_t, int64_t>(&query[pos] - start));
					goto in_dollar_quotes;
				}

				if (!IsValidDollarQuotedStringTagSubsequentChar(query[pos])) {
					// invalid char in dollar-quoted string, continue as normal
					goto regular;
				}
			}
			goto end;
		} else if (query[pos] == '-' && query[pos + 1] == '-') {
			goto in_comment;
		}
	}
	goto end;
in_quotes:
	for (; pos + 1 < qsize; pos++) {
		if (query[pos] == quote) {
			if (query[pos + 1] == quote) {
				// escaped quote
				pos++;
				continue;
			}
			pos++;
			goto regular;
		}
	}
	goto end;
in_dollar_quotes:
	for (; pos + 2 < qsize; pos++) {
		if (query[pos] == '$' &&
		    qsize - (pos + 1) >= dollar_quote_tag.GetSize() + 1 && // found '$' and enough space left
		    query[pos + dollar_quote_tag.GetSize() + 1] == '$' &&  // ending '$' at the right spot
		    memcmp(&query[pos + 1], dollar_quote_tag.GetData(), dollar_quote_tag.GetSize()) == 0) { // tags match
			pos += dollar_quote_tag.GetSize() + 1;
			goto regular;
		}
	}
	goto end;
in_comment:
	for (; pos < qsize; pos++) {
		if (query[pos] == '\n' || query[pos] == '\r') {
			goto regular;
		}
	}
	goto end;
end:
	return ReplaceUnicodeSpaces(query_str, new_query, unicode_spaces);
}

vector<string> SplitQueryStringIntoStatements(const string &query) {
	// Break sql string down into sql statements using the tokenizer
	vector<string> query_statements;
	auto tokens = Parser::Tokenize(query);
	idx_t next_statement_start = 0;
	for (idx_t i = 1; i < tokens.size(); ++i) {
		auto &t_prev = tokens[i - 1];
		auto &t = tokens[i];
		if (t_prev.type == SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR) {
			// LCOV_EXCL_START
			for (idx_t c = t_prev.start; c <= t.start; ++c) {
				if (query.c_str()[c] == ';') {
					query_statements.emplace_back(query.substr(next_statement_start, t.start - next_statement_start));
					next_statement_start = tokens[i].start;
				}
			}
			// LCOV_EXCL_STOP
		}
	}
	query_statements.emplace_back(query.substr(next_statement_start, query.size() - next_statement_start));
	return query_statements;
}

// 解析SQL查询的主函数，输入为查询字符串query DuckDB选择复用PostgreSQL的解析器, 然后用 Transformer 将 PostgreSQL 的解析树转换为 DuckDB 的语法树
void Parser::ParseQuery(const string &query) {
    // 创建语法树转换器，使用解析器配置选项
    Transformer transformer(options);
    string parser_error;             // 存储解析错误信息
    optional_idx parser_error_location; // 存储错误位置（可选值）

    // 阶段1：处理Unicode空格问题
    {
        string new_query;
        if (StripUnicodeSpaces(query, new_query)) { // 检测并去除Unicode空格（如全角空格）
            ParseQuery(new_query);  // 递归调用自身处理净化后的字符串
            return;                 // 直接返回，避免后续重复处理
        }
    }

    // 阶段2：主解析流程
    {
        // 设置Postgres解析器是否保留标识符大小写（如表名）
        PostgresParser::SetPreserveIdentifierCase(options.preserve_identifier_case);
        bool parsing_succeed = false;  // 主解析是否成功的标志

        // 通过代码块限制PostgresParser作用域，确保析构顺序避免内存问题
        {
            PostgresParser parser;        // 创建Postgres兼容解析器
            parser.Parse(query);          // 尝试解析原始查询

            if (parser.success) {          // 解析成功分支
                if (!parser.parse_tree) {  // 空语句检查（如仅含分号）
                    return;                // 直接返回，不生成语句
                }
                // 将Postgres语法树转换为DuckDB的SQLStatement对象
                transformer.TransformParseTree(parser.parse_tree, statements);
                parsing_succeed = true;    // 标记主解析成功
            } else {                      // 解析失败处理
                parser_error = parser.error_message;  // 记录错误信息
                if (parser.error_location > 0) {      // 定位错误字符位置
                    // 调整位置索引（Postgres从1开始，转为从0开始）
                    parser_error_location = NumericCast<idx_t>(parser.error_location - 1);
                }
            }
        }  // PostgresParser在此处析构

        // 阶段3：处理解析结果
        if (parsing_succeed) {
            // 主解析成功，无需操作（注释说明此处可能需要重构）
        } else if (!options.extensions || options.extensions->empty()) {
            // 无扩展解析器可用时，抛出标准语法错误
            throw ParserException::SyntaxError(query, parser_error, parser_error_location);
        } else {  // 存在扩展解析器的处理路径
            // 将查询按分号拆分为独立语句
            auto query_statements = SplitQueryStringIntoStatements(query);
            idx_t stmt_loc = 0;  // 语句在原始查询中的起始位置

            // 遍历每个拆分后的子语句
            for (auto const &query_statement : query_statements) {
                ErrorData another_parser_error;  // 扩展解析错误容器

                // 嵌套作用域确保another_parser正确析构
                {
                    PostgresParser another_parser;  // 新建解析器实例
                    another_parser.Parse(query_statement);  // 尝试解析子语句

                    // 检查是否DuckDB原生解析成功
                    if (another_parser.success) {
                        if (!another_parser.parse_tree) {  // 空语句跳过
                            continue;
                        }
                        // 转换语法树并记录语句元数据
                        transformer.TransformParseTree(another_parser.parse_tree, statements);
                        auto &last_stmt = statements.back();
                        last_stmt->stmt_length = query_statement.size() - 1;  // 语句长度
                        last_stmt->stmt_location = stmt_loc;                 // 起始位置
                        stmt_loc += query_statement.size();  // 更新全局位置计数器
                        continue;  // 处理下一个子语句
                    } else {  // 记录扩展解析错误
                        another_parser_error = ErrorData(another_parser.error_message);
                        if (another_parser.error_location > 0) {
                            another_parser_error.AddQueryLocation(
                                NumericCast<idx_t>(another_parser.error_location - 1));
                        }
                    }
                }  // another_parser析构

                // 阶段4：尝试使用扩展解析器
                bool parsed_single_statement = false;  // 扩展解析成功标志
                for (auto &ext : *options.extensions) { // 遍历所有注册的扩展
                    // 调用扩展的解析函数
                    auto result = ext.parse_function(ext.parser_info.get(), query_statement);

                    if (result.type == ParserExtensionResultType::PARSE_SUCCESSFUL) {
                        // 创建扩展专属的Statement对象
                        auto statement = make_uniq<ExtensionStatement>(ext, std::move(result.parse_data));
                        // 设置语句位置元数据
                        statement->stmt_length = query_statement.size() - 1;
                        statement->stmt_location = stmt_loc;
                        stmt_loc += query_statement.size();
                        statements.push_back(std::move(statement));  // 存储语句
                        parsed_single_statement = true;
                        break;  // 跳出扩展循环
                    } else if (result.type == ParserExtensionResultType::DISPLAY_EXTENSION_ERROR) {
                        // 扩展明确报错时抛出异常
                        throw ParserException::SyntaxError(query, result.error, result.error_location);
                    }
                    // 其他情况继续尝试下一个扩展
                }

                if (!parsed_single_statement) {  // 所有扩展均解析失败
                    throw ParserException::SyntaxError(query, parser_error, parser_error_location);
                }
            }
        }
    }  // 结束主处理块

    // 阶段5：后处理语句列表
    if (!statements.empty()) {
        auto &last_statement = statements.back();  // 处理最后一个语句
        // 修正最后一个语句的长度（可能因分号处理被截断）
        last_statement->stmt_length = query.size() - last_statement->stmt_location;

        // 为所有语句附加原始查询文本，并特别处理CREATE语句
        for (auto &statement : statements) {
            statement->query = query;  // 绑定原始SQL字符串
            if (statement->type == StatementType::CREATE_STATEMENT) {
                auto &create = statement->Cast<CreateStatement>();
                // 提取该语句对应的原始SQL片段（用于信息模式存储）
                create.info->sql = query.substr(statement->stmt_location, statement->stmt_length);
            }
        }
    }
}

vector<SimplifiedToken> Parser::Tokenize(const string &query) {
	auto pg_tokens = PostgresParser::Tokenize(query);
	vector<SimplifiedToken> result;
	result.reserve(pg_tokens.size());
	for (auto &pg_token : pg_tokens) {
		SimplifiedToken token;
		switch (pg_token.type) {
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_IDENTIFIER:
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER;
			break;
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT;
			break;
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_STRING_CONSTANT:
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT;
			break;
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_OPERATOR:
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR;
			break;
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_KEYWORD:
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD;
			break;
		// comments are not supported by our tokenizer right now
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_COMMENT: // LCOV_EXCL_START
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT;
			break;
		default:
			throw InternalException("Unrecognized token category");
		} // LCOV_EXCL_STOP
		token.start = NumericCast<idx_t>(pg_token.start);
		result.push_back(token);
	}
	return result;
}

vector<SimplifiedToken> Parser::TokenizeError(const string &error_msg) {
	idx_t error_start = 0;
	idx_t error_end = error_msg.size();

	vector<SimplifiedToken> tokens;
	// find "XXX Error:" - this marks the start of the error message
	auto error = StringUtil::Find(error_msg, "Error: ");
	if (error.IsValid()) {
		SimplifiedToken token;
		token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_ERROR;
		token.start = 0;
		tokens.push_back(token);

		token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER;
		token.start = error.GetIndex() + 6;
		tokens.push_back(token);

		error_start = error.GetIndex() + 7;
	}

	// find "LINE (number)" - this marks the end of the message
	auto line_pos = StringUtil::Find(error_msg, "\nLINE ");
	if (line_pos.IsValid()) {
		// tokenize between
		error_end = line_pos.GetIndex();
	}

	// now iterate over the
	bool in_quotes = false;
	char quote_char = '\0';
	for (idx_t i = error_start; i < error_end; i++) {
		if (in_quotes) {
			// in a quote - look for the quote character
			if (error_msg[i] == quote_char) {
				SimplifiedToken token;
				token.start = i;
				token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER;
				tokens.push_back(token);
				in_quotes = false;
			}
			if (StringUtil::CharacterIsNewline(error_msg[i])) {
				// found a newline in a quote, abort the quoted state entirely
				tokens.pop_back();
				in_quotes = false;
			}
		} else if (error_msg[i] == '"' || error_msg[i] == '\'') {
			// not quoted and found a quote - enter the quoted state
			SimplifiedToken token;
			token.start = i;
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT;
			token.start++;
			tokens.push_back(token);
			quote_char = error_msg[i];
			in_quotes = true;
		}
	}
	if (in_quotes) {
		// unterminated quotes at the end of the error - pop back the quoted state
		tokens.pop_back();
	}
	if (line_pos.IsValid()) {
		SimplifiedToken token;
		token.start = line_pos.GetIndex() + 1;
		token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT;
		tokens.push_back(token);

		// tokenize the LINE part
		idx_t query_start;
		for (query_start = line_pos.GetIndex() + 6; query_start < error_msg.size(); query_start++) {
			if (error_msg[query_start] != ':' && !StringUtil::CharacterIsDigit(error_msg[query_start])) {
				break;
			}
		}
		if (query_start < error_msg.size()) {
			token.start = query_start;
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER;
			tokens.push_back(token);

			idx_t query_end;
			for (query_end = query_start; query_end < error_msg.size(); query_end++) {
				if (error_msg[query_end] == '\n') {
					break;
				}
			}
			// after LINE XXX: comes a caret - look for it
			idx_t caret_position = error_msg.size();
			bool place_caret = false;
			idx_t caret_start = query_end + 1;
			if (caret_start < error_msg.size()) {
				for (idx_t i = caret_start; i < error_msg.size(); i++) {
					if (error_msg[i] == '^') {
						// found the caret
						// to get the caret position in the query we need to
						caret_position = i - caret_start - ((query_start - line_pos.GetIndex()) - 1);
						place_caret = true;
						break;
					}
				}
			}
			// tokenize the actual query
			string query = error_msg.substr(query_start, query_end - query_start);
			auto query_tokens = Tokenize(query);
			for (auto &query_token : query_tokens) {
				if (place_caret) {
					if (query_token.start >= caret_position) {
						// we need to place the caret here
						query_token.start = query_start + caret_position;
						query_token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_ERROR;
						tokens.push_back(query_token);

						place_caret = false;
						continue;
					}
				}
				query_token.start += query_start;
				tokens.push_back(query_token);
			}
			// FIXME: find the caret position and highlight/bold the identifier it points to
			if (query_end < error_msg.size()) {
				token.start = query_end;
				token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_ERROR;
				tokens.push_back(token);
			}
		}
	}
	return tokens;
}

KeywordCategory ToKeywordCategory(duckdb_libpgquery::PGKeywordCategory type) {
	switch (type) {
	case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_RESERVED:
		return KeywordCategory::KEYWORD_RESERVED;
	case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_UNRESERVED:
		return KeywordCategory::KEYWORD_UNRESERVED;
	case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_TYPE_FUNC:
		return KeywordCategory::KEYWORD_TYPE_FUNC;
	case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_COL_NAME:
		return KeywordCategory::KEYWORD_COL_NAME;
	case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_NONE:
		return KeywordCategory::KEYWORD_NONE;
	default:
		throw InternalException("Unrecognized keyword category");
	}
}

KeywordCategory Parser::IsKeyword(const string &text) {
	return ToKeywordCategory(PostgresParser::IsKeyword(text));
}

vector<ParserKeyword> Parser::KeywordList() {
	auto keywords = PostgresParser::KeywordList();
	vector<ParserKeyword> result;
	for (auto &kw : keywords) {
		ParserKeyword res;
		res.name = kw.text;
		res.category = ToKeywordCategory(kw.category);
		result.push_back(res);
	}
	return result;
}

vector<unique_ptr<ParsedExpression>> Parser::ParseExpressionList(const string &select_list, ParserOptions options) {
	// construct a mock query prefixed with SELECT
	string mock_query = "SELECT " + select_list;
	// parse the query
	Parser parser(options);
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = parser.statements[0]->Cast<SelectStatement>();
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = select.node->Cast<SelectNode>();
	return std::move(select_node.select_list);
}

GroupByNode Parser::ParseGroupByList(const string &group_by, ParserOptions options) {
	// construct a mock SELECT query with our group_by expressions
	string mock_query = StringUtil::Format("SELECT 42 GROUP BY %s", group_by);
	// parse the query
	Parser parser(options);
	parser.ParseQuery(mock_query);
	// check the result
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = parser.statements[0]->Cast<SelectStatement>();
	D_ASSERT(select.node->type == QueryNodeType::SELECT_NODE);
	auto &select_node = select.node->Cast<SelectNode>();
	return std::move(select_node.groups);
}

vector<OrderByNode> Parser::ParseOrderList(const string &select_list, ParserOptions options) {
	// construct a mock query
	string mock_query = "SELECT * FROM tbl ORDER BY " + select_list;
	// parse the query
	Parser parser(options);
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = parser.statements[0]->Cast<SelectStatement>();
	D_ASSERT(select.node->type == QueryNodeType::SELECT_NODE);
	auto &select_node = select.node->Cast<SelectNode>();
	if (select_node.modifiers.empty() || select_node.modifiers[0]->type != ResultModifierType::ORDER_MODIFIER ||
	    select_node.modifiers.size() != 1) {
		throw ParserException("Expected a single ORDER clause");
	}
	auto &order = select_node.modifiers[0]->Cast<OrderModifier>();
	return std::move(order.orders);
}

void Parser::ParseUpdateList(const string &update_list, vector<string> &update_columns,
                             vector<unique_ptr<ParsedExpression>> &expressions, ParserOptions options) {
	// construct a mock query
	string mock_query = "UPDATE tbl SET " + update_list;
	// parse the query
	Parser parser(options);
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::UPDATE_STATEMENT) {
		throw ParserException("Expected a single UPDATE statement");
	}
	auto &update = parser.statements[0]->Cast<UpdateStatement>();
	update_columns = std::move(update.set_info->columns);
	expressions = std::move(update.set_info->expressions);
}

vector<vector<unique_ptr<ParsedExpression>>> Parser::ParseValuesList(const string &value_list, ParserOptions options) {
	// construct a mock query
	string mock_query = "VALUES " + value_list;
	// parse the query
	Parser parser(options);
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = parser.statements[0]->Cast<SelectStatement>();
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = select.node->Cast<SelectNode>();
	if (!select_node.from_table || select_node.from_table->type != TableReferenceType::EXPRESSION_LIST) {
		throw ParserException("Expected a single VALUES statement");
	}
	auto &values_list = select_node.from_table->Cast<ExpressionListRef>();
	return std::move(values_list.values);
}

ColumnList Parser::ParseColumnList(const string &column_list, ParserOptions options) {
	string mock_query = "CREATE TABLE tbl (" + column_list + ")";
	Parser parser(options);
	parser.ParseQuery(mock_query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw ParserException("Expected a single CREATE statement");
	}
	auto &create = parser.statements[0]->Cast<CreateStatement>();
	if (create.info->type != CatalogType::TABLE_ENTRY) {
		throw InternalException("Expected a single CREATE TABLE statement");
	}
	auto &info = create.info->Cast<CreateTableInfo>();
	return std::move(info.columns);
}

} // namespace duckdb
