//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/planner.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "parser/sql_statement.hpp"

#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! The planner creates a logical query plan from the parsed SQL statements
//! using the Binder and LogicalPlanGenerator.
class Planner {
  public:
	void CreatePlan(ClientContext &catalog, std::unique_ptr<SQLStatement> statement);

	std::unique_ptr<BindContext> context;
	std::unique_ptr<LogicalOperator> plan;

  private:
	void CreatePlan(ClientContext &, SQLStatement &statement);
};
} // namespace duckdb
