//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/commit_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "transaction/undo_buffer.hpp"

namespace duckdb {
class DataChunk;
class DataTable;
class WriteAheadLog;

struct DeleteInfo;
struct UpdateInfo;

template <bool HAS_LOG> class CommitState {
public:
	CommitState(transaction_t commit_id, WriteAheadLog *log = nullptr);

	WriteAheadLog *log;
	transaction_t commit_id;
	UndoFlags current_op;

	DataTable *current_table;
	index_t row_identifiers[STANDARD_VECTOR_SIZE];

	unique_ptr<DataChunk> delete_chunk;
	unique_ptr<DataChunk> update_chunk;
public:
	void CommitEntry(UndoFlags type, data_ptr_t data);

private:
	void SwitchTable(DataTable *table, UndoFlags new_op);

	void WriteCatalogEntry(CatalogEntry *entry);
	void WriteDelete(DeleteInfo *info);
	void WriteUpdate(UpdateInfo *info);

	void AppendRowId(row_t rowid);
};

} // namespace duckdb
