#include "storage/string_segment.hpp"
#include "storage/buffer_manager.hpp"
// #include "common/types/vector.hpp"
// #include "storage/table/append_state.hpp"
#include "transaction/update_info.hpp"
// #include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

StringSegment::StringSegment(ColumnData &column_data, BufferManager &manager) :
	UncompressedSegment(column_data, manager, TypeId::VARCHAR) {
	this->max_vector_count = 0;
	this->dictionary_offset = 0;
	// the vector_size is given in the size of the dictionary offsets
	this->vector_size = STANDARD_VECTOR_SIZE * sizeof(int32_t) + sizeof(nullmask_t);
	this->string_updates = nullptr;

	// we allocate one block to hold the majority of the
	auto handle = manager.Allocate(BLOCK_SIZE);
	this->block_id = handle->block_id;

	ExpandStringSegment(handle->buffer->data.get());
}

void StringSegment::ExpandStringSegment(data_ptr_t baseptr) {
	// clear the nullmask for this vector
	auto mask = (nullmask_t*) (baseptr + (max_vector_count * vector_size));
	mask->reset();

	max_vector_count++;
	if (versions) {
		auto new_versions = unique_ptr<UpdateInfo*[]>(new UpdateInfo*[max_vector_count]);
		memcpy(new_versions.get(), versions.get(), (max_vector_count - 1) * sizeof(UpdateInfo*));
		new_versions[max_vector_count - 1] = nullptr;
		versions = move(new_versions);
	}

	if (string_updates) {
		auto new_string_updates = unique_ptr<string_update_info_t[]>(new string_update_info_t[max_vector_count]);
		for(index_t i = 0; i < max_vector_count - 1; i++) {
			new_string_updates[i] = move(string_updates[i]);
		}
		new_string_updates[max_vector_count - 1] = 0;
		string_updates = move(new_string_updates);
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void StringSegment::InitializeScan(TransientScanState &state) {
	// pin the primary buffer
	state.primary_handle = manager.PinBuffer(block_id);
}

void StringSegment::Scan(Transaction &transaction, TransientScanState &state, index_t vector_index, Vector &result) {
	auto read_lock = lock.GetSharedLock();

	// clear any previously locked buffers and get the primary buffer handle
	auto handle = (ManagedBufferHandle*) state.primary_handle.get();
	state.handles.clear();

	index_t count = GetVectorCount(vector_index);

	// fetch the data from the base segment
	FetchBaseData(state, handle->buffer->data.get(), vector_index, result, count);
	if (versions && versions[vector_index]) {
		// fetch data from updates
		auto result_data = (char**) result.data;
		auto current = versions[vector_index];
		while(current) {
			if (current->version_number > transaction.start_time && current->version_number != transaction.transaction_id) {
				// these tuples were either committed AFTER this transaction started or are not committed yet, use tuples stored in this version
				auto info_data = (string_location_t*) current->tuple_data;
				for(index_t i = 0; i < current->N; i++) {
					auto string = FetchString(state, handle->buffer->data.get(), info_data[i]);
					result_data[current->tuples[i]] = string.data;
					result.nullmask[current->tuples[i]] = current->nullmask[current->tuples[i]];
				}
			}
			current = current->next;
		}
	}
}

void StringSegment::FetchStringLocations(row_t *ids, index_t vector_index, index_t vector_offset, index_t count, string_location_t result[], nullmask_t &result_nullmask) {
	// first pin the base block
	auto handle = manager.PinBuffer(block_id);

	auto baseptr = handle->buffer->data.get();
	auto base = baseptr + vector_index * vector_size;
	auto &base_nullmask = *((nullmask_t*) base);
	auto base_data = (int32_t *) (base + sizeof(nullmask_t));

	if (string_updates && string_updates[vector_index]) {
		// there are updates: merge them in
		auto &info = *string_updates[vector_index];
		index_t update_idx = 0;
		for(index_t i = 0; i < count; i++) {
			auto id = ids[i] - vector_offset;
			while(update_idx < info.count && info.ids[update_idx] < id) {
				update_idx++;
			}
			if (update_idx < info.count && info.ids[update_idx] == id) {
				// use update info
				result[i].block_id = info.block_ids[update_idx];
				result[i].offset = info.offsets[update_idx];
				result_nullmask[id] = info.nullmask[info.ids[update_idx]];
				update_idx++;
			} else {
				// use base table info
				result[i] = FetchStringLocation(baseptr, base_data[id]);
				result_nullmask[id] = base_nullmask[id];
			}
		}
	} else {
		// no updates: fetch strings from base vector
		for(index_t i = 0; i < count; i++) {
			auto id = ids[i] - vector_offset;
			result[i] = FetchStringLocation(baseptr, base_data[id]);
			result_nullmask[id] = base_nullmask[id];
		}
	}
}

void StringSegment::FetchBaseData(TransientScanState &state, data_ptr_t baseptr, index_t vector_index, Vector &result, index_t count) {
	auto base = baseptr + vector_index * vector_size;

	auto &base_nullmask = *((nullmask_t*) base);
	auto base_data = (int32_t *) (base + sizeof(nullmask_t));
	auto result_data = (char**) result.data;

	if (string_updates && string_updates[vector_index]) {
		// there are updates: merge them in
		auto &info = *string_updates[vector_index];
		index_t update_idx = 0;
		for(index_t i = 0; i < count; i++) {
			if (update_idx < info.count && info.ids[update_idx] == i) {
				// use update info
				result_data[i] = ReadString(state, info.block_ids[update_idx], info.offsets[update_idx]).data;
				result.nullmask[i] = info.nullmask[info.ids[update_idx]];
				update_idx++;
			} else {
				// use base table info
				result_data[i] = FetchStringFromDict(state, baseptr, base_data[i]).data;
				result.nullmask[i] = base_nullmask[i];
			}
		}
	} else {
		// no updates: fetch only from the string dictionary
		for(index_t i = 0; i < count; i++) {
			result_data[i] = FetchStringFromDict(state, baseptr, base_data[i]).data;
		}
		result.nullmask = base_nullmask;
	}
	result.count = count;
}

string_location_t StringSegment::FetchStringLocation(data_ptr_t baseptr, int32_t dict_offset) {
	if (dict_offset == 0) {
		return string_location_t(INVALID_BLOCK, 0);
	}
	// look up result in dictionary
	auto dict_end = baseptr + BLOCK_SIZE;
	auto dict_pos = dict_end - dict_offset;
	auto string_length = *((uint16_t*) dict_pos);
	string_location_t result;
	if (string_length == BIG_STRING_MARKER) {
		ReadStringMarker(dict_pos, result.block_id, result.offset);
	} else {
		result.block_id = INVALID_BLOCK;
		result.offset = dict_offset;
	}
	return result;
}

string_t StringSegment::FetchStringFromDict(TransientScanState &state, data_ptr_t baseptr, int32_t dict_offset) {
	// fetch base data
	string_location_t location = FetchStringLocation(baseptr, dict_offset);
	return FetchString(state, baseptr, location);
}

string_t StringSegment::FetchString(TransientScanState &state, data_ptr_t baseptr, string_location_t location) {
	if (location.block_id != INVALID_BLOCK) {
		// big string marker: read from separate block
		return ReadString(state, location.block_id, location.offset);
	} else {
		if (location.offset == 0) {
			return string_t(nullptr, 0);
		}
		// normal string: read string from this block
		auto dict_end = baseptr + BLOCK_SIZE;
		auto dict_pos = dict_end - location.offset;
		auto string_length = *((uint16_t*) dict_pos);

		string_t result;
		result.length = string_length;
		result.data = (char*) (dict_pos + sizeof(uint16_t));
		return result;
	}

}

void StringSegment::IndexScan(TransientScanState &state, index_t vector_index, Vector &result) {
	throw Exception("FIXME");
}

void StringSegment::Fetch(index_t vector_index, Vector &result) {
	throw Exception("FIXME");
}

void StringSegment::Fetch(Transaction &transaction, row_t row_id, Vector &result) {
	throw Exception("FIXME");
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
index_t StringSegment::Append(SegmentStatistics &stats, TransientAppendState &state, Vector &data, index_t offset, index_t count) {
	assert(data.type == TypeId::VARCHAR);
	auto handle = manager.PinBuffer(block_id);

	index_t initial_count = tuple_count;
	while(count > 0) {
		// get the vector index of the vector to append to and see how many tuples we can append to that vector
		index_t vector_index = tuple_count / STANDARD_VECTOR_SIZE;
		if (vector_index == max_vector_count) {
			// we are at the maximum vector, check if there is space to increase the maximum vector count
			// as a heuristic, we only allow another vector to be added if we have at least 32 bytes per string remaining (32KB out of a 256KB block, or around 12% empty)
			index_t remaining_space = BLOCK_SIZE - dictionary_offset - max_vector_count * vector_size;
			if (remaining_space >= STANDARD_VECTOR_SIZE * 32) {
				// we have enough remaining space to add another vector
				ExpandStringSegment(handle->buffer->data.get());
			} else {
				break;
			}
		}
		index_t current_tuple_count = tuple_count - vector_index * STANDARD_VECTOR_SIZE;
		index_t append_count = std::min(STANDARD_VECTOR_SIZE - current_tuple_count, count);

		// now perform the actual append
		AppendData(stats, handle->buffer->data.get() + vector_size * vector_index, handle->buffer->data.get() + BLOCK_SIZE, current_tuple_count, data, offset, append_count);

		count -= append_count;
		offset += append_count;
		tuple_count += append_count;
	}
	return tuple_count - initial_count;
}

void StringSegment::AppendData(SegmentStatistics &stats, data_ptr_t target, data_ptr_t end, index_t target_offset, Vector &source, index_t offset, index_t count) {
	assert(offset + count <= source.count);
	auto ldata = (char**)source.data;
	auto &result_nullmask = *((nullmask_t*) target);
	auto result_data = (int32_t *) (target + sizeof(nullmask_t));

	VectorOperations::Exec(source.sel_vector, count + offset, [&](index_t i, index_t k) {
		if (source.nullmask[i]) {
			// null value is stored as -1
			result_data[k - offset + target_offset] = 0;
			result_nullmask[k - offset + target_offset] = true;
			stats.has_null = true;
		} else {
			// non-null value, check if we can fit it within the block
			// we also always store strings that have a size >= STRING_BLOCK_LIMIT in the overflow blocks
			index_t string_length = strlen(ldata[i]);
			index_t total_length = string_length + 1 + sizeof(uint16_t);

			if (string_length > stats.max_string_length) {
				stats.max_string_length = string_length;
			}
			if (total_length >= STRING_BLOCK_LIMIT || total_length > BLOCK_SIZE - dictionary_offset - max_vector_count * vector_size) {
				// string is too big for block: write to overflow blocks
				block_id_t block;
				int32_t offset;
				// write the string into the current string block
				WriteString(string_t(ldata[i], string_length + 1), block, offset);

				dictionary_offset += BIG_STRING_MARKER_SIZE;
				auto dict_pos = end - dictionary_offset;

				// write a big string marker into the dictionary
				WriteStringMarker(dict_pos, block, offset);
			} else {
				// string fits in block, append to dictionary and increment dictionary position
				assert(string_length < std::numeric_limits<uint16_t>::max());
				dictionary_offset += total_length;
				auto dict_pos = end - dictionary_offset;

				// first write the length as u16
				uint16_t string_length_u16 = string_length;
				memcpy(dict_pos, &string_length_u16, sizeof(uint16_t));
				// now write the actual string data into the dictionary
				memcpy(dict_pos + sizeof(uint16_t), ldata[i], string_length + 1);
			}
			// place the dictionary offset into the set of vectors
			result_data[k - offset + target_offset] = dictionary_offset;
		}
	}, offset);
}

void StringSegment::WriteString(string_t string, block_id_t &result_block, int32_t &result_offset) {
	uint32_t total_length = string.length + 1 + sizeof(uint32_t);
	unique_ptr<ManagedBufferHandle> handle;
	// check if the string fits in the current block
	if (!head || head->offset + total_length >= head->size) {
		// string does not fit, allocate space for it
		// create a new string block
		index_t alloc_size = std::max((index_t) total_length, (index_t) BLOCK_SIZE);
		auto new_block = make_unique<StringBlock>();
		new_block->offset = 0;
		new_block->size = alloc_size;
		// allocate an in-memory buffer for it
		handle = manager.Allocate(alloc_size);
		new_block->block_id = handle->block_id;
		new_block->next = move(head);
		head = move(new_block);
	} else {
		// string fits, copy it into the current block
		handle = manager.PinBuffer(head->block_id);
	}

	result_block = head->block_id;
	result_offset = head->offset;

	// copy the string and the length there
	auto ptr = handle->buffer->data.get() + head->offset;
	memcpy(ptr, &string.length, sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(ptr, string.data, string.length + 1);
	head->offset += total_length;
}

string_t StringSegment::ReadString(TransientScanState &state, block_id_t block, int32_t offset) {
	if (block == INVALID_BLOCK) {
		return string_t(nullptr, 0);
	}
	// now pin the handle, if it is not pinned yet
	ManagedBufferHandle *handle;
	auto entry = state.handles.find(block);
	if (entry == state.handles.end()) {
		auto pinned_handle = manager.PinBuffer(block);
		handle = pinned_handle.get();

		state.handles.insert(make_pair(block, move(pinned_handle)));
	} else {
		handle = (ManagedBufferHandle*) entry->second.get();
	}
	return ReadString(handle->buffer->data.get(), offset);
}

string_t StringSegment::ReadString(data_ptr_t target, int32_t offset) {
	auto ptr = target + offset;
	string_t result;
	result.length = *((uint32_t*) ptr);
	result.data = (char*) (ptr + sizeof(uint32_t));
	return result;
}

void StringSegment::WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset) {
	uint16_t length = BIG_STRING_MARKER;
	memcpy(target, &length, sizeof(uint16_t));
	target += sizeof(uint16_t);
	memcpy(target, &block_id, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(target, &offset, sizeof(int32_t));
}

void StringSegment::ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset) {
	target += sizeof(uint16_t);
	memcpy(&block_id, target, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(&offset, target, sizeof(int32_t));
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
string_update_info_t StringSegment::CreateStringUpdate(SegmentStatistics &stats, Vector &update, row_t *ids, index_t vector_offset) {
	auto info = make_unique<StringUpdateInfo>();
	info->count = update.count;
	auto strings = (char**) update.data;
	for(index_t i = 0; i < update.count; i++) {
		info->ids[i] = ids[i] - vector_offset;
		info->nullmask[info->ids[i]] = update.nullmask[i];
		// copy the string into the block
		if (!update.nullmask[i]) {
			WriteString(string_t(strings[i], strlen(strings[i]) + 1), info->block_ids[i], info->offsets[i]);
		} else {
			info->block_ids[i] = INVALID_BLOCK;
			info->offsets[i] = 0;
		}
	}
	return info;
}

void StringSegment::Update(SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids, index_t vector_index, index_t vector_offset, UpdateInfo *node) {
	if (!string_updates) {
		string_updates = unique_ptr<string_update_info_t[]>(new string_update_info_t[max_vector_count]);
	}

	// fetch the original string locations
	string_location_t string_locations[STANDARD_VECTOR_SIZE];
	nullmask_t original_nullmask;
	FetchStringLocations(ids, vector_index, vector_offset, update.count, string_locations, original_nullmask);

	string_update_info_t new_update_info;
	// next up: create the updates
	if (!string_updates[vector_index]) {
		// no string updates yet, allocate a block and place the updates there
		new_update_info = CreateStringUpdate(stats, update, ids, vector_offset);
	} else {
		throw Exception("FIXME: merge string update");
	}

	// now that the original strings are placed in the undo buffer and the updated strings are placed in the base table
	// create the update node
	if (!node) {
		// create a new node in the undo buffer for this update
		node = CreateUpdateInfo(transaction, ids, update.count, vector_index, vector_offset, sizeof(string_location_t));

		// copy the string location data into the undo buffer
		node->nullmask = original_nullmask;
		memcpy(node->tuple_data, string_locations, sizeof(string_location_t) * update.count);
	} else {
		throw Exception("FIXME: merge update");
	}
	// finally move the string updates in place
	string_updates[vector_index] = move(new_update_info);
}

void StringSegment::RollbackUpdate(UpdateInfo *info) {
	auto lock_handle = lock.GetExclusiveLock();

	if (string_updates[info->vector_index]->count == info->N) {
#ifdef DEBUG
		for(index_t i = 0; i < info->N; i++) {
			assert(info->tuples[i] == string_updates[info->vector_index]->ids[i]);
		}
#endif
		string_updates[info->vector_index].reset();
	} else {
		throw Exception("FIXME: only remove part of the update");
	}
	// auto &update_info = *string_updates[info->vector_index];
	// auto string_locations = (string_location_t*) info->tuple_data;
	// for(index_t i = 0; i < info->N; i++) {
	// 	update_info.block_ids[info->tuples[i]] = string_locations[i].block_id;
	// 	update_info.offsets[info->tuples[i]] = string_locations[i].offset;
	// }
	CleanupUpdate(info);
}
