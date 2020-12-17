//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_show.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalShow : public LogicalOperator {
public:
	LogicalShow(unique_ptr<LogicalOperator> plan) : LogicalOperator(LogicalOperatorType::SHOW) {
		children.push_back(move(plan));
	}

  vector<SQLType> types_select;
	vector<string> aliases;

protected:
	void ResolveTypes() override {
		types = {TypeId::VARCHAR, TypeId::VARCHAR};
	}
};
} // namespace duckdb
