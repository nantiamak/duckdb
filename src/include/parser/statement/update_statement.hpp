//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/update_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"

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

	unique_ptr<ParsedExpression> condition;
	unique_ptr<TableRef> table;

	vector<string> columns;
	vector<unique_ptr<ParsedExpression>> expressions;
};
} // namespace duckdb
