//===----------------------------------------------------------------------===//
//                         DuckDB
//
// ../../../src/include/duckdb/planner/statement/bound_execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/prepared_statement_catalog_entry.hpp"
#include "duckdb/planner/bound_sql_statement.hpp"

namespace duckdb {
//! Bound equivalent to ExecuteStatement
class BoundExecuteStatement : public BoundSQLStatement {
public:
	BoundExecuteStatement() : BoundSQLStatement(StatementType::EXECUTE) {
	}

	//! The prepared statement to execute
	PreparedStatementCatalogEntry *prep;

public:
	vector<string> GetNames() override {
		return prep->prepared->names;
	}
	vector<SQLType> GetTypes() override {
		return prep->prepared->sql_types;
	}
};
} // namespace duckdb
