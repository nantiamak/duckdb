//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/numeric_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/uncompressed_segment.hpp"

namespace duckdb {

struct StringBlock {
	block_id_t block_id;
	index_t offset;
	index_t size;
	unique_ptr<StringBlock> next;
};

struct string_location_t {
	string_location_t(block_id_t block_id, int32_t offset) : block_id(block_id), offset(offset) {}
	string_location_t(){}

	block_id_t block_id;
	int32_t offset;
};

struct StringUpdateInfo {
	sel_t count;
	sel_t ids[STANDARD_VECTOR_SIZE];
	block_id_t block_ids[STANDARD_VECTOR_SIZE];
	int32_t offsets[STANDARD_VECTOR_SIZE];
};

typedef unique_ptr<StringUpdateInfo> string_update_info_t;

class StringSegment : public UncompressedSegment {
public:
	StringSegment(BufferManager &manager, index_t row_start, block_id_t block_id = INVALID_BLOCK);

	//! The current dictionary offset
	index_t dictionary_offset;
	//! The string block holding strings that do not fit in the main block
	//! FIXME: this should be replaced by a heap that also allows freeing of unused strings
	unique_ptr<StringBlock> head;
	//! Blocks that hold string updates (if any)
	unique_ptr<string_update_info_t[]> string_updates;
public:
	void InitializeScan(ColumnScanState &state) override;

	//! Fetch a single value and append it to the vector
	void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result) override;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is full.
	index_t Append(SegmentStatistics &stats, Vector &data, index_t offset, index_t count) override;

	//! Rollback a previous update
	void RollbackUpdate(UpdateInfo *info) override;
protected:
	void Update(ColumnData &column_data, SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids, index_t vector_index, index_t vector_offset, UpdateInfo *node) override;

	void FetchBaseData(ColumnScanState &state, index_t vector_index, Vector &result) override;
	void FetchUpdateData(ColumnScanState &state, Transaction &transaction, UpdateInfo *versions, Vector &result) override;
private:
	void AppendData(SegmentStatistics &stats, data_ptr_t target, data_ptr_t end, index_t target_offset, Vector &source, index_t offset, index_t count);

	//! Fetch all the strings of a vector from the base table and place their locations in the result vector
	void FetchBaseData(ColumnScanState &state, data_ptr_t base_data, index_t vector_index, Vector &result, index_t count);

	string_location_t FetchStringLocation(data_ptr_t baseptr, int32_t dict_offset);
	string_t FetchString(buffer_handle_set_t &handles, data_ptr_t baseptr, string_location_t location);
	//! Fetch a single string from the dictionary and returns it, potentially pins a buffer manager page and adds it to the set of pinned pages
	string_t FetchStringFromDict(buffer_handle_set_t &handles, data_ptr_t baseptr, int32_t dict_offset);

	//! Fetch string locations for a subset of the strings
	void FetchStringLocations(data_ptr_t baseptr, row_t *ids, index_t vector_index, index_t vector_offset, index_t count, string_location_t result[]);

	void WriteString(string_t string, block_id_t &result_block, int32_t &result_offset);
	string_t ReadString(buffer_handle_set_t &handles, block_id_t block, int32_t offset);
	string_t ReadString(data_ptr_t target, int32_t offset);

	void WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset);
	void ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset);

	//! Expand the string segment, adding an additional maximum vector to the segment
	void ExpandStringSegment(data_ptr_t baseptr);

	string_update_info_t CreateStringUpdate(SegmentStatistics &stats, Vector &update, row_t *ids, index_t vector_offset);
	string_update_info_t MergeStringUpdate(SegmentStatistics &stats, Vector &update, row_t *ids, index_t vector_offset, StringUpdateInfo &update_info);

	void MergeUpdateInfo(UpdateInfo *node, Vector &update, row_t *ids, index_t vector_offset, string_location_t string_locations[], nullmask_t original_nullmask);
private:
	//! The max string size that is allowed within a block. Strings bigger than this will be labeled as a BIG STRING and offloaded to the overflow blocks.
	static constexpr uint16_t STRING_BLOCK_LIMIT = 4096;
	//! Marker used in length field to indicate the presence of a big string
	static constexpr uint16_t BIG_STRING_MARKER = (uint16_t) -1;
	//! The marker size of the big string
	static constexpr index_t BIG_STRING_MARKER_SIZE = sizeof(block_id_t) + sizeof(int32_t) + sizeof(uint16_t);
};

}
