//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/update_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

#include <vector>

namespace duckdb {

class UpdateStatement : public SQLStatement {
public:
	UpdateStatement() : SQLStatement(StatementType::UPDATE) {
	}
	string ToString() const override {
		return "Update";
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	unique_ptr<Expression> condition;
	unique_ptr<TableRef> table;

	vector<string> columns;
	vector<unique_ptr<Expression>> expressions;
};
} // namespace duckdb
