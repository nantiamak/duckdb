//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/persistent_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/table/column_segment.hpp"
#include "storage/block.hpp"
#include "storage/buffer_manager.hpp"

#include "common/unordered_map.hpp"

namespace duckdb {

class PersistentSegment : public ColumnSegment {
public:
	PersistentSegment(BufferManager &manager, block_id_t id, index_t offset, TypeId type, index_t start, index_t count);

	//! The buffer manager
	BufferManager &manager;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The offset into the block
	index_t offset;
public:
	void InitializeScan(ColumnPointer &pointer) override;
	void Scan(ColumnPointer &pointer, Vector &result, index_t count) override;
	void Scan(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector, index_t sel_count) override;
	void Fetch(Vector &result, index_t row_id) override;
private:
	//! Lock for big strings
	std::mutex big_string_lock;
	//! Big string map
	unordered_map<block_id_t, block_id_t> big_strings;

	Block *PinHandle(ColumnPointer &pointer);

	void AppendFromStorage(ColumnPointer &pointer, Block *block, Vector &source, Vector &target, bool has_null);

	template <bool HAS_NULL> void AppendStrings(ColumnPointer &pointer, Block *block, Vector &source, Vector &target);

	const char *GetBigString(ColumnPointer &pointer, block_id_t block);
};

} // namespace duckdb
