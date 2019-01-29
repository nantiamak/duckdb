//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

struct JoinCondition {
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
	ExpressionType comparison;
};

enum JoinSide { NONE, LEFT, RIGHT, BOTH };

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
public:
	LogicalJoin(JoinType type) :
		LogicalOperator(LogicalOperatorType::JOIN), type(type) {
	}

	vector<string> GetNames() override;

	//! The conditions of the join
	vector<JoinCondition> conditions;
	//! The type of the join (INNER, OUTER, etc...)
	JoinType type;
	//! Whether or not the join is a special "duplicate eliminated" join. This join type is only used for subquery flattening, and involves performing duplicate elimination on the LEFT side which is then pushed into the RIGHT side. 
	bool is_duplicate_eliminated;

	string ParamsToString() const override;

	size_t ExpressionCount() override;
	Expression *GetExpression(size_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                       size_t index) override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
