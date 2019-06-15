//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_expression_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalExpressionGet represents a scan operation over a set of to-be-executed expressions
class LogicalExpressionGet : public LogicalOperator {
public:
	LogicalExpressionGet(index_t table_index, vector<TypeId> types, vector<vector<unique_ptr<Expression>>> expressions)
	    : LogicalOperator(LogicalOperatorType::EXPRESSION_GET), table_index(table_index), expr_types(types), expressions(move(expressions)) {
	}

	//! The table index in the current bind context
	index_t table_index;
	//! The types of the expressions
	vector<TypeId> expr_types;
	//! The set of expressions
	vector<vector<unique_ptr<Expression>>> expressions;
protected:
	void ResolveTypes() override {
		// types are resolved in the constructor
		this->types = expr_types;
	}
};
} // namespace duckdb
