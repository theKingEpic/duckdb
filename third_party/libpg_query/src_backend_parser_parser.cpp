/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - raw_parser
 * - base_yylex
 * - raw_parser
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * parser.c
 *		Main entry point/driver for PostgreSQL grammar
 *
 * Note that the grammar is not allowed to perform any table access
 * (since we need to be able to do basic parsing even while inside an
 * aborted transaction).  Therefore, the data structures returned by
 * the grammar are "raw" parsetrees that still need to be analyzed by
 * analyze.c and related files.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/parser/parser.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_functions.hpp"

#include "parser/gramparse.hpp"
#include "parser/parser.hpp"
#include "parser/kwlist.hpp"

namespace duckdb_libpgquery {

/*
 * raw_parser
 *		Given a query in string form, do lexical and grammatical analysis.
 *
 * Returns a list of raw (un-analyzed) parse trees.  The immediate elements
 * of the list are always PGRawStmt nodes.
 */
// 声明函数：将SQL字符串解析为抽象语法树（AST），返回PGList指针（PostgreSQL的链表结构）
PGList *raw_parser(const char *str) {
	// Flex词法分析器的状态机对象，用于跟踪扫描过程（如当前扫描位置、缓冲区状态等）
	core_yyscan_t yyscanner;
	// 自定义结构体，用于在词法分析器（Flex）和语法分析器（Bison）之间传递数据
	base_yy_extra_type yyextra;
	// 语法解析结果：0表示成功，非0表示失败
	int yyresult;

	/* 初始化Flex词法分析器 */
	// scanner_init：配置Flex词法分析器，传入输入字符串、关键字表等参数
	yyscanner = scanner_init(
		str,                     // 输入的SQL字符串
		&yyextra.core_yy_extra,  // 传递给词法分析器的核心数据（如关键字、扫描状态）
		ScanKeywords,            // 预定义的SQL关键字哈希表（如SELECT, FROM等）
		NumScanKeywords          // 关键字的数量
	);

	/* 初始化语法分析器的预读标记状态 */
	// 标记当前没有预读的token（Bison语法解析可能需要预读一个token来处理歧义语法）
	yyextra.have_lookahead = false;

	/* 初始化语法分析器（Bison）的上下文 */
	// 重置parsetree为空链表，准备存储解析后的语法树节点
	parser_init(&yyextra);

	/* 执行语法解析！ */
	// 调用Bison生成的语法分析函数base_yyparse，启动解析过程
	yyresult = base_yyparse(yyscanner);  // 返回值：0成功，非0失败

	/* 清理词法分析器资源 */
	// 释放Flex词法分析器的内部缓冲区及状态机内存
	scanner_finish(yyscanner);

	// 处理解析结果
	if (yyresult) {
		/* 解析失败（如语法错误），返回空链表NIL（PostgreSQL中表示空结果的惯用方式） */
		return NIL;
	} else {
		/* 解析成功，返回生成的语法树（存储在yyextra.parsetree中） */
		return yyextra.parsetree;  // PGList链表，每个节点代表一个AST元素（如SELECT语句、表达式等）
	}
}

PGKeywordCategory is_keyword(const char *text) {
	auto keyword = ScanKeywordLookup(text, ScanKeywords, NumScanKeywords);
	if (keyword) {
		return static_cast<PGKeywordCategory>(keyword->category);
	}
	return PGKeywordCategory::PG_KEYWORD_NONE;
}

std::vector<PGKeyword> keyword_list() {
    std::vector<PGKeyword> result;
	for(size_t i = 0; i < NumScanKeywords; i++) {
		PGKeyword keyword;
		keyword.text = ScanKeywords[i].name;
		switch(ScanKeywords[i].category) {
		case UNRESERVED_KEYWORD:
			keyword.category = PGKeywordCategory::PG_KEYWORD_UNRESERVED;
			break;
		case RESERVED_KEYWORD:
			keyword.category = PGKeywordCategory::PG_KEYWORD_RESERVED;
			break;
		case TYPE_FUNC_NAME_KEYWORD:
			keyword.category = PGKeywordCategory::PG_KEYWORD_TYPE_FUNC;
			break;
		case COL_NAME_KEYWORD:
			keyword.category = PGKeywordCategory::PG_KEYWORD_COL_NAME;
			break;
		}
		result.push_back(keyword);
	}
	return result;
}

std::vector<PGSimplifiedToken> tokenize(const char *str) {
	core_yyscan_t yyscanner;
	base_yy_extra_type yyextra;

	std::vector<PGSimplifiedToken> result;
	yyscanner = scanner_init(str, &yyextra.core_yy_extra, ScanKeywords, NumScanKeywords);
	yyextra.have_lookahead = false;

	while(true) {
		YYSTYPE type;
		YYLTYPE loc;
		int token;
		try {
			token = base_yylex(&type, &loc, yyscanner);
		} catch(...) {
			token = 0;
		}
		if (token == 0) {
			break;
		}
		PGSimplifiedToken current_token;
		switch(token) {
		case IDENT:
			current_token.type = PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_IDENTIFIER;
			break;
		case ICONST:
		case FCONST:
			current_token.type = PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_NUMERIC_CONSTANT;
			break;
		case SCONST:
		case BCONST:
		case XCONST:
			current_token.type = PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_STRING_CONSTANT;
			break;
		case Op:
		case PARAM:
		case COLON_EQUALS:
		case EQUALS_GREATER:
		case LESS_EQUALS:
		case GREATER_EQUALS:
		case NOT_EQUALS:
			current_token.type = PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_OPERATOR;
			break;
		default:
			if (token >= 255) {
				// non-ascii value, probably a keyword
				current_token.type = PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_KEYWORD;
			} else {
				// ascii value, probably an operator
				current_token.type = PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_OPERATOR;
			}
			break;
		}
		current_token.start = loc;
		result.push_back(current_token);
	}

	scanner_finish(yyscanner);
	return result;
}



/*
 * Intermediate filter between parser and core lexer (core_yylex in scan.l).
 *
 * This filter is needed because in some cases the standard SQL grammar
 * requires more than one token lookahead.  We reduce these cases to one-token
 * lookahead by replacing tokens here, in order to keep the grammar LALR(1).
 *
 * Using a filter is simpler than trying to recognize multiword tokens
 * directly in scan.l, because we'd have to allow for comments between the
 * words.  Furthermore it's not clear how to do that without re-introducing
 * scanner backtrack, which would cost more performance than this filter
 * layer does.
 *
 * The filter also provides a convenient place to translate between
 * the core_YYSTYPE and YYSTYPE representations (which are really the
 * same thing anyway, but notationally they're different).
 */
int base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner) {
	base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
	int cur_token;
	int next_token;
	int cur_token_length;
	YYLTYPE cur_yylloc;

	/* Get next token --- we might already have it */
	if (yyextra->have_lookahead) {
		cur_token = yyextra->lookahead_token;
		lvalp->core_yystype = yyextra->lookahead_yylval;
		*llocp = yyextra->lookahead_yylloc;
		*(yyextra->lookahead_end) = yyextra->lookahead_hold_char;
		yyextra->have_lookahead = false;
	} else
		cur_token = core_yylex(&(lvalp->core_yystype), llocp, yyscanner);

	/*
	 * If this token isn't one that requires lookahead, just return it.  If it
	 * does, determine the token length.  (We could get that via strlen(), but
	 * since we have such a small set of possibilities, hardwiring seems
	 * feasible and more efficient.)
	 */
	switch (cur_token) {
	case NOT:
		cur_token_length = 3;
		break;
	case NULLS_P:
		cur_token_length = 5;
		break;
	case WITH:
		cur_token_length = 4;
		break;
	default:
		return cur_token;
	}

	/*
	 * Identify end+1 of current token.  core_yylex() has temporarily stored a
	 * '\0' here, and will undo that when we call it again.  We need to redo
	 * it to fully revert the lookahead call for error reporting purposes.
	 */
	yyextra->lookahead_end = yyextra->core_yy_extra.scanbuf + *llocp + cur_token_length;
	Assert(*(yyextra->lookahead_end) == '\0');

	/*
	 * Save and restore *llocp around the call.  It might look like we could
	 * avoid this by just passing &lookahead_yylloc to core_yylex(), but that
	 * does not work because flex actually holds onto the last-passed pointer
	 * internally, and will use that for error reporting.  We need any error
	 * reports to point to the current token, not the next one.
	 */
	cur_yylloc = *llocp;

	/* Get next token, saving outputs into lookahead variables */
	next_token = core_yylex(&(yyextra->lookahead_yylval), llocp, yyscanner);
	yyextra->lookahead_token = next_token;
	yyextra->lookahead_yylloc = *llocp;

	*llocp = cur_yylloc;

	/* Now revert the un-truncation of the current token */
	yyextra->lookahead_hold_char = *(yyextra->lookahead_end);
	*(yyextra->lookahead_end) = '\0';

	yyextra->have_lookahead = true;

	/* Replace cur_token if needed, based on lookahead */
	switch (cur_token) {
	case NOT:
		/* Replace NOT by NOT_LA if it's followed by BETWEEN, IN, etc */
		switch (next_token) {
		case BETWEEN:
		case IN_P:
		case LIKE:
		case ILIKE:
		case SIMILAR:
			cur_token = NOT_LA;
			break;
		}
		break;

	case NULLS_P:
		/* Replace NULLS_P by NULLS_LA if it's followed by FIRST or LAST */
		switch (next_token) {
		case FIRST_P:
		case LAST_P:
			cur_token = NULLS_LA;
			break;
		}
		break;

	case WITH:
		/* Replace WITH by WITH_LA if it's followed by TIME or ORDINALITY */
		switch (next_token) {
		case TIME:
		case ORDINALITY:
			cur_token = WITH_LA;
			break;
		}
		break;
	}

	return cur_token;
}

}
