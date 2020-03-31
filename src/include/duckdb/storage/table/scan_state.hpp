//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/scan_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {
class LocalTableStorage;
class Index;
class PersistentSegment;
class TransientSegment;

struct IndexScanState {
	vector<column_t> column_ids;

	IndexScanState(vector<column_t> column_ids) : column_ids(column_ids) {
	}
	virtual ~IndexScanState() {
	}
};

typedef unordered_map<block_id_t, unique_ptr<BufferHandle>> buffer_handle_set_t;

struct ColumnScanState {
	//! The column segment that is currently being scanned
	ColumnSegment *current;
	//! The vector index of the transient segment
	idx_t vector_index;
	//! The primary buffer handle
	unique_ptr<BufferHandle> primary_handle;
	//! The set of pinned block handles for this scan
	buffer_handle_set_t handles;
	//! The locks that are held during the scan, only used by the index scan
	vector<unique_ptr<StorageLockKey>> locks;
	//! Whether or not InitializeState has been called for this segment
	bool initialized;

public:
	//! Move on to the next vector in the scan
	void Next();
	//! Move on to next segment
	void NextSegment();
};

struct ColumnFetchState {
	//! The set of pinned block handles for this set of fetches
	buffer_handle_set_t handles;
};

struct LocalScanState {
	LocalTableStorage *storage = nullptr;

	idx_t chunk_index;
	idx_t max_index;
	idx_t last_chunk_count;
};

struct TableScanState {
	virtual ~TableScanState() = default;

	idx_t current_persistent_row, max_persistent_row;
	idx_t current_transient_row, max_transient_row;
	unique_ptr<ColumnScanState[]> column_scans;
	idx_t offset;
	vector<column_t> column_ids;
	LocalScanState local_state;
};

struct CreateIndexScanState : public TableScanState {
	vector<unique_ptr<StorageLockKey>> locks;
	std::unique_lock<std::mutex> append_lock;
};

struct TableIndexScanState {
	Index *index;
	unique_ptr<IndexScanState> index_state;
	ColumnFetchState fetch_state;
	LocalScanState local_state;
	vector<column_t> column_ids;
};

} // namespace duckdb
