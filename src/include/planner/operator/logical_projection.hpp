//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_projection.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalProjection represents the projection list in a SELECT clause
class LogicalProjection : public LogicalOperator {
  public:
	LogicalProjection(std::vector<std::unique_ptr<Expression>> select_list)
	    : LogicalOperator(LogicalOperatorType::PROJECTION,
	                      std::move(select_list)) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
};
} // namespace duckdb
