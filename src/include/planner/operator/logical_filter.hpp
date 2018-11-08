//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_filter.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalFilter represents a filter operation (e.g. WHERE or HAVING clause)
class LogicalFilter : public LogicalOperator {
  public:
	LogicalFilter(std::unique_ptr<Expression> expression);
	LogicalFilter();

	virtual void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}

	//! Splits up the predicates of the LogicalFilter into a set of predicates
	//! separated by AND Returns whether or not any splits were made
	bool SplitPredicates();
};

} // namespace duckdb
