//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/alter_table_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"
#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"

namespace duckdb {

class AlterTableStatement : public SQLStatement {
public:
	AlterTableStatement(unique_ptr<AlterTableInformation> info)
	    : SQLStatement(StatementType::ALTER), info(std::move(info)){};

	unique_ptr<TableRef> table;
	unique_ptr<AlterTableInformation> info;
};

} // namespace duckdb
