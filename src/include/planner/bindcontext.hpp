//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/bindcontext.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "parser/column_definition.hpp"
#include "parser/parsed_expression.hpp"
#include "parser/expression/bound_columnref_expression.hpp"
#include "parser/sql_statement.hpp"
#include "planner/expression_binder.hpp"

namespace duckdb {
enum class BindingType : uint8_t { DUMMY = 0, TABLE = 1, SUBQUERY = 2, TABLE_FUNCTION = 3 };

struct Binding {
	Binding(BindingType type, size_t index) : type(type), index(index) {
	}
	virtual ~Binding() {
	}

	BindingType type;
	size_t index;
};

struct DummyTableBinding : public Binding {
	std::unordered_map<string, ColumnDefinition *> bound_columns;

	DummyTableBinding(vector<ColumnDefinition> &columns) : Binding(BindingType::DUMMY, 0) {
		for (auto &it : columns) {
			bound_columns[it.name] = &it;
		}
	}
	virtual ~DummyTableBinding() {
	}
};

struct TableBinding : public Binding {
	TableCatalogEntry *table;

	TableBinding(TableCatalogEntry *table, size_t index) : Binding(BindingType::TABLE, index), table(table) {
	}
	virtual ~TableBinding() {
	}
};

struct SubqueryBinding : public Binding {
	QueryNode *subquery;
	//! Column names of the subquery
	vector<string> names;
	//! Name -> index for the names
	std::unordered_map<string, size_t> name_map;

	SubqueryBinding(SubqueryRef &subquery_, size_t index);
	SubqueryBinding(QueryNode *select_, size_t index);

	virtual ~SubqueryBinding() {
	}
};

struct TableFunctionBinding : public Binding {
	TableFunctionCatalogEntry *function;

	TableFunctionBinding(TableFunctionCatalogEntry *function, size_t index)
	    : Binding(BindingType::TABLE_FUNCTION, index), function(function) {
	}
};

//! The BindContext object keeps track of all the tables and columns that are
//! encountered during the binding process.
class BindContext {
public:
	BindContext();

	//! Given a column name, find the matching table it belongs to. Throws an
	//! exception if no table has a column of the given name.
	string GetMatchingBinding(const string &column_name);
	//! Binds a column expression to the base table. Returns the bound expression
	//! or throws an exception if the column could not be bound.
	BindResult BindColumn(unique_ptr<Expression> expr, uint32_t depth);

	//! Generate column expressions for all columns that are present in the
	//! referenced tables. This is used to resolve the * expression in a
	//! selection list.
	void GenerateAllColumnExpressions(vector<unique_ptr<Expression>> &new_select_list);

	//! Adds a base table with the given alias to the BindContext.
	void AddBaseTable(size_t index, const string &alias, TableCatalogEntry *table_entry);
	//! Adds a subquery with a given alias to the BindContext.
	void AddSubquery(size_t index, const string &alias, SubqueryRef &subquery);
	//! Adds a table function with a given alias to the BindContext
	void AddTableFunction(size_t index, const string &alias, TableFunctionCatalogEntry *function_entry);
	//! Adds a dummy table with the given set of columns to the BindContext.
	void AddDummyTable(const string &alias, vector<ColumnDefinition> &columns);

	//! Returns true if the table/subquery alias exists, false otherwise.
	bool HasAlias(const string &alias);
	//! Gets the binding index of a given alias. Throws an exception if the
	//! alias was not found.
	size_t GetBindingIndex(const string &alias);
	//! Returns the maximum depth of column references in the current context

	//! The set of columns that are bound for each table/subquery alias
	std::unordered_map<string, vector<string>> bound_columns;

private:
	void AddBinding(const string &alias, unique_ptr<Binding> binding);

	//! The set of bindings
	std::unordered_map<string, unique_ptr<Binding>> bindings;
	//! The list of bindings in insertion order
	vector<std::pair<string, Binding *>> bindings_list;
};
} // namespace duckdb
