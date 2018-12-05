#include "parser/expression/columnref_expression.hpp"
#include "parser/statement/create_index_statement.hpp"
#include "parser/tableref/basetableref.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<CreateIndexStatement> Transformer::TransformCreateIndex(Node *node) {
	IndexStmt *stmt = reinterpret_cast<IndexStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateIndexStatement>();
	auto &info = *result->info.get();

	info.unique = stmt->unique;

	for (auto cell = stmt->indexParams->head; cell != nullptr; cell = cell->next) {
		auto index_element = (IndexElem *)cell->data.ptr_value;
		if (index_element->collation) {
			throw NotImplementedException("Index with collation not supported yet!");
		}
		if (index_element->opclass) {
			throw NotImplementedException("Index with opclass not supported yet!");
		}

		if (index_element->name) {
			// create a column reference expression
			result->expressions.push_back(make_unique<ColumnRefExpression>(index_element->name));
		} else {
			// parse the index expression
			assert(index_element->expr);
			result->expressions.push_back(TransformExpression(index_element->expr));
		}
	}

	info.index_type = StringToIndexType(string(stmt->accessMethod));
	auto tableref = make_unique<BaseTableRef>();
	tableref->table_name = stmt->relation->relname;
	if (stmt->relation->schemaname) {
		tableref->schema_name = stmt->relation->schemaname;
	}
	result->table = move(tableref);
	if (stmt->idxname) {
		info.index_name = stmt->idxname;
	} else {
		throw NotImplementedException("Index wout a name not supported yet!");
	}
	return result;
}
