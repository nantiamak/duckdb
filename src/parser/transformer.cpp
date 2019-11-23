#include "duckdb/parser/transformer.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/statement/list.hpp"

using namespace duckdb;
using namespace std;

bool Transformer::TransformParseTree(PGList *tree, vector<unique_ptr<SQLStatement>> &statements) {
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		auto stmt = TransformStatement((PGNode *)entry->data.ptr_value);
		if (!stmt) {
			statements.clear();
			return false;
		}
		statements.push_back(move(stmt));
	}
	return true;
}

unique_ptr<SQLStatement> Transformer::TransformStatement(PGNode *stmt) {
	switch (stmt->type) {
	case T_PGRawStmt:
		return TransformStatement(((PGRawStmt *)stmt)->stmt);
	case T_PGSelectStmt:
		return TransformSelect(stmt);
	case T_PGCreateStmt:
		return TransformCreateTable(stmt);
	case T_PGCreateSchemaStmt:
		return TransformCreateSchema(stmt);
	case T_PGViewStmt:
		return TransformCreateView(stmt);
	case T_PGCreateSeqStmt:
		return TransformCreateSequence(stmt);
	case T_PGDropStmt:
		return TransformDrop(stmt);
	case T_PGInsertStmt:
		return TransformInsert(stmt);
	case T_PGCopyStmt:
		return TransformCopy(stmt);
	case T_PGTransactionStmt:
		return TransformTransaction(stmt);
	case T_PGDeleteStmt:
		return TransformDelete(stmt);
	case T_PGUpdateStmt:
		return TransformUpdate(stmt);
	case T_PGIndexStmt:
		return TransformCreateIndex(stmt);
	case T_PGAlterTableStmt:
		return TransformAlter(stmt);
	case T_PGRenameStmt:
		return TransformRename(stmt);
	case T_PGPrepareStmt:
		return TransformPrepare(stmt);
	case T_PGExecuteStmt:
		return TransformExecute(stmt);
	case T_PGDeallocateStmt:
		return TransformDeallocate(stmt);
	case T_PGCreateTableAsStmt:
		return TransformCreateTableAs(stmt);
	case T_PGExplainStmt: {
		PGExplainStmt *explain_stmt = reinterpret_cast<PGExplainStmt *>(stmt);
		return make_unique<ExplainStatement>(TransformStatement(explain_stmt->query));
	}
	case T_PGVacuumStmt: { // Ignore VACUUM/ANALYZE for now
		return nullptr;
	}
	case T_PGPragmaStmt:
		return TransformPragma(stmt);
	default:
		throw NotImplementedException(NodetypeToString(stmt->type));
	}
	return nullptr;
}
