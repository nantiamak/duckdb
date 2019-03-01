//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_empty_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"
#include "parser/expression/bound_columnref_expression.hpp"

namespace duckdb {

//! LogicalEmptyResult returns an empty result. This is created by the optimizer if it can reason that ceratin parts of the tree will always return an empty result.
class LogicalEmptyResult : public LogicalOperator {
public:
	LogicalEmptyResult(unique_ptr<LogicalOperator> op);

	vector<TypeId> return_types;
	//! The names of the result set
	vector<string> names;
	//! The tables that would be bound at this location (if the subtree was not optimized away)
	vector<BoundTable> bound_tables;

	vector<string> GetNames() override {
		return names;
	}

protected:
	void ResolveTypes() override {
		this->types = return_types;
	}
};
} // namespace duckdb
