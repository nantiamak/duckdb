//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/sql_statement.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "common/printable.hpp"

namespace duckdb {
class SelectStatement;
class SQLNodeVisitor;

//! SQLStatement is the base class of any type of SQL statement.
class SQLStatement : public Printable {
  public:
	SQLStatement(StatementType type) : type(type){};
	virtual ~SQLStatement() {
	}

	virtual std::unique_ptr<SQLStatement> Accept(SQLNodeVisitor *) = 0;
	virtual bool Equals(const SQLStatement *other) {
		return other && type == other->type;
	}

	StatementType type;
};
} // namespace duckdb
