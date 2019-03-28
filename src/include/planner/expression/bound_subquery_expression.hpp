//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/binder.hpp"
#include "planner/bound_query_node.hpp"
#include "planner/expression.hpp"

namespace duckdb {

class BoundSubqueryExpression : public Expression {
public:
	BoundSubqueryExpression(TypeId return_type, SQLType sql_type = SQLType());

	bool IsCorrelated() {
		return binder->correlated_columns.size() > 0;
	}

	//! The binder used to bind the subquery node
	unique_ptr<Binder> binder;
	//! The bound subquery node
	unique_ptr<BoundQueryNode> subquery;
	//! The subquery type
	SubqueryType subquery_type;
	//! the child expression to compare with (in case of IN, ANY, ALL operators)
	unique_ptr<Expression> child;
	//! The comparison type of the child expression with the subquery (in case of ANY, ALL operators)
	ExpressionType comparison_type;

public:
	bool HasSubquery() const override {
		return true;
	}
	bool IsScalar() const override {
		return false;
	}

	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
