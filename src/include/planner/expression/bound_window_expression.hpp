//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_window_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/window_expression.hpp"
#include "planner/bound_query_node.hpp"
#include "planner/expression.hpp"

namespace duckdb {

class BoundWindowExpression : public Expression {
public:
	BoundWindowExpression(ExpressionType type, TypeId return_type, SQLType sql_type);

	//! The child expression of the main window aggregate
	unique_ptr<Expression> child;
	//! The set of expressions to partition by
	vector<unique_ptr<Expression>> partitions;
	//! The set of ordering clauses
	vector<BoundOrderByNode> orders;
	//! The window boundaries
	WindowBoundary start = WindowBoundary::INVALID;
	WindowBoundary end = WindowBoundary::INVALID;

	unique_ptr<Expression> start_expr;
	unique_ptr<Expression> end_expr;
	//! Offset and default expressions for WINDOW_LEAD and WINDOW_LAG functions
	unique_ptr<Expression> offset_expr;
	unique_ptr<Expression> default_expr;

public:
	bool IsWindow() const override {
		return true;
	}

	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
