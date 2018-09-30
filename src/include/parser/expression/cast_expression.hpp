//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/cast_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {
//! Represents a type cast from one type to another type
class CastExpression : public Expression {
  public:
	CastExpression(TypeId target, std::unique_ptr<Expression> child)
	    : Expression(ExpressionType::OPERATOR_CAST, target, std::move(child)) {}

	virtual void ResolveType() override {
		Expression::ResolveType();
		stats = children[0]->stats;
		if (!stats.FitsInType(return_type)) {
			return_type = stats.MinimalType();
		}
	}

	virtual std::string ToString() const override {
		return "CAST[" + TypeIdToString(return_type) + "](" +
		       children[0]->ToString() + ")";
	}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
};
} // namespace duckdb
