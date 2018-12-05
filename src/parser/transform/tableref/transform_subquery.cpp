#include "parser/tableref/subqueryref.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<TableRef> Transformer::TransformRangeSubselect(RangeSubselect *root) {
	auto subquery = TransformSelectNode((SelectStmt *)root->subquery);
	if (!subquery) {
		return nullptr;
	}
	auto result = make_unique<SubqueryRef>(move(subquery));
	result->alias = TransformAlias(root->alias);
	if (root->alias->colnames) {
		for (auto node = root->alias->colnames->head; node != nullptr; node = node->next) {
			result->column_name_alias.push_back(reinterpret_cast<value *>(node->data.ptr_value)->val.str);
		}
		if (result->column_name_alias.size() != result->subquery->GetSelectList().size()) {
			throw ParserException("Column alias list count does not match");
		}
	}
	return move(result);
}
