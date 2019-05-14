//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_distinct.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalDistinct filters duplicate entries from its child operator
class LogicalDistinct : public LogicalOperator {
public:
	LogicalDistinct() : LogicalOperator(LogicalOperatorType::DISTINCT) {
	}
	LogicalDistinct(vector<unique_ptr<Expression>> targets)
	    : LogicalOperator(LogicalOperatorType::DISTINCT, move(targets)) {
	}
	//! The set of distinct targets (optional).
	vector<unique_ptr<Expression>> distinct_targets;

	count_t ExpressionCount() override;
	Expression *GetExpression(index_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                       index_t index) override;

	string ParamsToString() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
