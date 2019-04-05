//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "parser/parsed_expression.hpp"
#include "parser/tokens.hpp"
#include "planner/expression.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class SelectNode;

struct BindResult {
	BindResult(string error) : error(error) {
	}
	BindResult(unique_ptr<Expression> expr, SQLType sql_type) : expression(move(expr)), sql_type(sql_type) {
	}

	bool HasError() {
		return !error.empty();
	}

	unique_ptr<Expression> expression;
	SQLType sql_type;
	string error;
};

//! BoundExpression is an intermediate dummy class used by the binder. It is a ParsedExpression but holds an Expression.
//! It represents a successfully bound expression. It is used in the Binder to prevent re-binding of already bound parts
//! when dealing with subqueries.
class BoundExpression : public ParsedExpression {
public:
	BoundExpression(unique_ptr<Expression> expr, SQLType sql_type)
	    : ParsedExpression(ExpressionType::INVALID, ExpressionClass::BOUND_EXPRESSION), expr(move(expr)),
	      sql_type(sql_type) {
	}

	unique_ptr<Expression> expr;
	SQLType sql_type;

public:
	string ToString() const override {
		return "BOUND_EXPRESSION";
	}

	unique_ptr<ParsedExpression> Copy() const override {
		throw SerializationException("Cannot copy or serialize bound expression");
	}
};

class ExpressionBinder {
public:
	ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder = false);
	virtual ~ExpressionBinder();

	unique_ptr<Expression> Bind(unique_ptr<ParsedExpression> &expr, SQLType *result_type = nullptr,
	                            bool root_expression = true);

	//! Returns whether or not any columns have been bound by the expression binder
	bool BoundColumns() {
		return bound_columns;
	}

	string Bind(unique_ptr<ParsedExpression> *expr, uint32_t depth, bool root_expression = false);

	// Bind table names to ColumnRefExpressions
	void BindTableNames(ParsedExpression &expr);

	bool BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr);

	//! The target type that should result from the binder. If the result is not of this type, a cast to this type will
	//! be added. Defaults to INVALID.
	SQLType target_type;

protected:
	virtual BindResult BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression = false);

	BindResult BindExpression(CaseExpression &expr, uint32_t depth);
	BindResult BindExpression(CastExpression &expr, uint32_t depth);
	BindResult BindExpression(ColumnRefExpression &expr, uint32_t depth);
	BindResult BindExpression(ComparisonExpression &expr, uint32_t depth);
	BindResult BindExpression(ConjunctionExpression &expr, uint32_t depth);
	BindResult BindExpression(ConstantExpression &expr, uint32_t depth);
	BindResult BindExpression(FunctionExpression &expr, uint32_t depth);
	BindResult BindExpression(OperatorExpression &expr, uint32_t depth);
	BindResult BindExpression(ParameterExpression &expr, uint32_t depth);
	BindResult BindExpression(StarExpression &expr, uint32_t depth);
	BindResult BindExpression(SubqueryExpression &expr, uint32_t depth);

	void BindChild(unique_ptr<ParsedExpression> &expr, uint32_t depth, string &error);

protected:
	static void ExtractCorrelatedExpressions(Binder &binder, Expression &expr);

	Binder &binder;
	ClientContext &context;
	ExpressionBinder *stored_binder;
	bool bound_columns = false;
};

//! Cast an expression to the specified SQL type if required
unique_ptr<Expression> AddCastToType(unique_ptr<Expression> expr, SQLType source_type, SQLType target_type);
} // namespace duckdb
