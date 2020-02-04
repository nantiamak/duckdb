//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bind_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/table_binding.hpp"

namespace duckdb {
class BoundQueryNode;

//! The BindContext object keeps track of all the tables and columns that are
//! encountered during the binding process.
class BindContext {
public:
	//! Given a column name, find the matching table it belongs to. Throws an
	//! exception if no table has a column of the given name.
	string GetMatchingBinding(const string &column_name);

	Binding* GetCTEBinding(const string &ctename);
	//! Binds a column expression to the base table. Returns the bound expression
	//! or throws an exception if the column could not be bound.
	BindResult BindColumn(ColumnRefExpression &colref, index_t depth);

	//! Generate column expressions for all columns that are present in the
	//! referenced tables. This is used to resolve the * expression in a
	//! selection list.
	void GenerateAllColumnExpressions(vector<unique_ptr<ParsedExpression>> &new_select_list);

	//! Adds a base table with the given alias to the BindContext.
	void AddBaseTable(BoundBaseTableRef *bound, const string &alias);
	//! Adds a subquery with a given alias to the BindContext.
	void AddSubquery(index_t index, const string &alias, SubqueryRef &ref, BoundQueryNode &subquery);
	//! Adds a table function with a given alias to the BindContext
	void AddTableFunction(index_t index, const string &alias, TableFunctionCatalogEntry *function_entry);
	//! Adds a base table with the given alias to the BindContext.
	void AddGenericBinding(index_t index, const string &alias, vector<string> names, vector<SQLType> types);

    //! Adds a base table with the given alias to the CTE BindContext.
    //! We need this to correctly bind recursive CTEs with multiple references.
    void AddCTEBinding(index_t index, const string &alias, vector<string> names, vector<SQLType> types);


    unordered_set<string> hidden_columns;

    //! Keep track of recursive CTE references
    unordered_map<string, std::shared_ptr<index_t>> cte_references;

    unordered_map<string, std::shared_ptr<Binding>> GetCTEBindings() { return cte_bindings; }
    void SetCTEBindings(unordered_map<string, std::shared_ptr<Binding>> bindings) { cte_bindings = bindings; }

private:
	void AddBinding(const string &alias, unique_ptr<Binding> binding);

	//! The set of bindings
	unordered_map<string, unique_ptr<Binding>> bindings;
	//! The list of bindings in insertion order
	vector<std::pair<string, Binding *>> bindings_list;

    //! The set of CTE bindings
    unordered_map<string, std::shared_ptr<Binding>> cte_bindings;
};
} // namespace duckdb
