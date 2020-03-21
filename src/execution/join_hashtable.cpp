#include "duckdb/execution/join_hashtable.hpp"

#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

using namespace std;

namespace duckdb {

using ScanStructure = JoinHashTable::ScanStructure;

JoinHashTable::JoinHashTable(BufferManager &buffer_manager, vector<JoinCondition> &conditions,
                             vector<TypeId> build_types, JoinType type)
    : buffer_manager(buffer_manager), build_types(build_types), equality_size(0), condition_size(0), build_size(0),
      entry_size(0), tuple_size(0), join_type(type), finalized(false), has_null(false), count(0) {
	for (auto &condition : conditions) {
		assert(condition.left->return_type == condition.right->return_type);
		auto type = condition.left->return_type;
		auto type_size = GetTypeIdSize(type);
		if (condition.comparison == ExpressionType::COMPARE_EQUAL) {
			// all equality conditions should be at the front
			// all other conditions at the back
			// this assert checks that
			assert(equality_types.size() == condition_types.size());
			equality_types.push_back(type);
			equality_size += type_size;
		}
		predicates.push_back(condition.comparison);
		null_values_are_equal.push_back(condition.null_values_are_equal);
		assert(!condition.null_values_are_equal ||
		       (condition.null_values_are_equal && condition.comparison == ExpressionType::COMPARE_EQUAL));

		condition_types.push_back(type);
		condition_size += type_size;
	}
	// at least one equality is necessary
	assert(equality_types.size() > 0);

	if (type == JoinType::ANTI || type == JoinType::SEMI || type == JoinType::MARK) {
		// for ANTI, SEMI and MARK join, we only need to store the keys
		build_size = 0;
	} else {
		// otherwise we need to store the entire build side for reconstruction
		// purposes
		for (idx_t i = 0; i < build_types.size(); i++) {
			build_size += GetTypeIdSize(build_types[i]);
		}
	}
	tuple_size = condition_size + build_size;
	// entry size is the tuple size and the size of the hash/next pointer
	entry_size = tuple_size + std::max(sizeof(uint64_t), sizeof(void *));
	// compute the per-block capacity of this HT
	block_capacity = std::max((idx_t)STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / entry_size) + 1);
}

JoinHashTable::~JoinHashTable() {
	if (hash_map) {
		auto hash_id = hash_map->block_id;
		hash_map.reset();
		buffer_manager.DestroyBuffer(hash_id);
	}
	pinned_handles.clear();
	for (auto &block : blocks) {
		buffer_manager.DestroyBuffer(block.block_id);
	}
}

void JoinHashTable::ApplyBitmask(Vector &hashes, idx_t count) {
	if (hashes.vector_type == VectorType::CONSTANT_VECTOR) {
		assert(!ConstantVector::IsNull(hashes));
		auto indices = ConstantVector::GetData<uint64_t>(hashes);
		*indices = *indices & bitmask;
	} else {
		hashes.Normalify(count);
		auto indices = FlatVector::GetData<uint64_t>(hashes);
		for(idx_t i = 0; i < count; i++) {
			indices[i] &= bitmask;
		}
	}
}

void JoinHashTable::ApplyBitmask(Vector &hashes, const SelectionVector &sel, idx_t count, Vector &pointers) {
	VectorData hdata;
	hashes.Orrify(count, hdata);

	auto hash_data = (uint64_t *) hdata.data;
	auto result_data = FlatVector::GetData<data_ptr_t*>(pointers);
	auto main_ht = (data_ptr_t *) hash_map->node->buffer;
	for(idx_t i = 0; i < count; i++) {
		auto rindex = sel.get_index(i);
		auto hindex = hdata.sel->get_index(i);
		auto hash = hash_data[hindex];
		result_data[rindex] = main_ht + (hash & bitmask);
	}
}

void JoinHashTable::Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes) {
	if (count == keys.size()) {
		// no null values are filtered: use regular hash functions
		VectorOperations::Hash(keys.data[0], hashes, keys.size());
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], keys.size());
		}
	} else {
		// null values were filtered: use selection vector
		VectorOperations::Hash(keys.data[0], hashes, sel, count);
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], sel, count);
		}
	}
}
template<class T>
static void templated_serialize_vdata(VectorData &vdata, const SelectionVector &sel, idx_t count, data_ptr_t key_locations[]) {
	auto source = (T*) vdata.data;
	if (vdata.nullmask->any()) {
		for(idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			auto target = (T*) key_locations[i];
			if ((*vdata.nullmask)[source_idx]) {
				*target = NullValue<T>();
			} else {
				*target = source[source_idx];
			}
			key_locations[i] += sizeof(T);
		}
	} else {
		for(idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			auto target = (T*) key_locations[i];
			*target = source[source_idx];
			key_locations[i] += sizeof(T);
		}
	}
}

void JoinHashTable::SerializeVectorData(VectorData &vdata, TypeId type, const SelectionVector &sel, idx_t count, data_ptr_t key_locations[]) {
	switch (type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		templated_serialize_vdata<int8_t>(vdata, sel, count, key_locations);
		break;
	case TypeId::INT16:
		templated_serialize_vdata<int16_t>(vdata, sel, count, key_locations);
		break;
	case TypeId::INT32:
		templated_serialize_vdata<int32_t>(vdata, sel, count, key_locations);
		break;
	case TypeId::INT64:
		templated_serialize_vdata<int64_t>(vdata, sel, count, key_locations);
		break;
	case TypeId::FLOAT:
		templated_serialize_vdata<float>(vdata, sel, count, key_locations);
		break;
	case TypeId::DOUBLE:
		templated_serialize_vdata<double>(vdata, sel, count, key_locations);
		break;
	case TypeId::HASH:
		templated_serialize_vdata<uint64_t>(vdata, sel, count, key_locations);
		break;
	case TypeId::VARCHAR: {
		auto source = (string_t *) vdata.data;
		for(idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx);

			auto target = (string_t*) key_locations[i];
			if ((*vdata.nullmask)[source_idx]) {
				*target = NullValue<string_t>();
			} else if (source[source_idx].IsInlined()) {
				*target = source[source_idx];
			} else {
				*target = string_heap.AddString(source[source_idx]);
			}
			key_locations[i] += sizeof(string_t);
		}
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented serialize");
	}
}

void JoinHashTable::SerializeVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t count, data_ptr_t key_locations[]) {
	VectorData vdata;
	v.Orrify(vcount, vdata);

	SerializeVectorData(vdata, v.type, sel, count, key_locations);
}

idx_t JoinHashTable::AppendToBlock(HTDataBlock &block, BufferHandle &handle, idx_t count, data_ptr_t key_locations[], idx_t remaining) {
	idx_t append_count = std::min(remaining, block.capacity - block.count);
	auto dataptr = handle.node->buffer + block.count * entry_size;
	idx_t offset = count - remaining;
	for(idx_t i = 0; i < append_count; i++) {
		key_locations[offset + i] = dataptr;
		dataptr += entry_size;
	}
	block.count += append_count;
	return append_count;
}

static idx_t FilterNullValues(VectorData &vdata, const SelectionVector &sel, idx_t count, SelectionVector &result) {
	auto &nullmask = *vdata.nullmask;
	idx_t result_count = 0;
	for(idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto key_idx = vdata.sel->get_index(idx);
		if (!nullmask[key_idx]) {
			result.set_index(result_count++, idx);
		}
	}
	return result_count;
}


idx_t JoinHashTable::PrepareKeys(DataChunk &keys, unique_ptr<VectorData[]> &key_data, const SelectionVector *&current_sel, SelectionVector &sel) {
	key_data = unique_ptr<VectorData[]>(new VectorData[keys.column_count()]);
	for(idx_t key_idx = 0; key_idx < keys.column_count(); key_idx++) {
		keys.data[key_idx].Orrify(keys.size(), key_data[key_idx]);
	}

	// figure out which keys are NULL, and create a selection vector out of them
	current_sel = &FlatVector::IncrementalSelectionVector;
	idx_t added_count = keys.size();
	for(idx_t i = 0; i < keys.column_count(); i++) {
		if (!null_values_are_equal[i]) {
			if (!key_data[i].nullmask->any()) {
				continue;
			}
			added_count = FilterNullValues(key_data[i], *current_sel, added_count, sel);
			// null values are NOT equal for this column, filter them out
			current_sel = &sel;
		}
	}
	return added_count;
}

void JoinHashTable::Build(DataChunk &keys, DataChunk &payload) {
	assert(!finalized);
	assert(keys.size() == payload.size());
	if (keys.size() == 0) {
		return;
	}

	// prepare the keys for processing
	unique_ptr<VectorData[]> key_data;
	const SelectionVector *current_sel;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t added_count = PrepareKeys(keys, key_data, current_sel, sel);
	if (added_count == 0) {
		return;
	}
	count += added_count;

	// special case: correlated mark join
	if (join_type == JoinType::MARK && correlated_mark_join_info.correlated_types.size() > 0) {
		auto &info = correlated_mark_join_info;
		// Correlated MARK join
		// for the correlated mark join we need to keep track of COUNT(*) and COUNT(COLUMN) for each of the correlated
		// columns push into the aggregate hash table
		assert(info.correlated_counts);
		info.group_chunk.SetCardinality(keys);
		for (idx_t i = 0; i < info.correlated_types.size(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		info.payload_chunk.SetCardinality(keys);
		for (idx_t i = 0; i < 2; i++) {
			info.payload_chunk.data[i].Reference(keys.data[info.correlated_types.size()]);
			info.payload_chunk.data[i].type = TypeId::INT64;
		}
		info.correlated_counts->AddChunk(info.group_chunk, info.payload_chunk);
	}

	vector<unique_ptr<BufferHandle>> handles;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	// first allocate space of where to serialize the keys and payload columns
	idx_t remaining = added_count;
	// first append to the last block (if any)
	if (blocks.size() != 0) {
		auto &last_block = blocks.back();
		if (last_block.count < last_block.capacity) {
			// last block has space: pin the buffer of this block
			auto handle = buffer_manager.Pin(last_block.block_id);
			// now append to the block
			idx_t append_count = AppendToBlock(last_block, *handle, remaining, key_locations, remaining);
			remaining -= append_count;
			handles.push_back(move(handle));
		}
	}
	while (remaining > 0) {
		// now for the remaining data, allocate new buffers to store the data and append there
		auto handle = buffer_manager.Allocate(block_capacity * entry_size);

		HTDataBlock new_block;
		new_block.count = 0;
		new_block.capacity = block_capacity;
		new_block.block_id = handle->block_id;

		idx_t append_count = AppendToBlock(new_block, *handle, remaining, key_locations, remaining);
		remaining -= append_count;
		handles.push_back(move(handle));
		blocks.push_back(new_block);
	}

	// hash the keys and obtain an entry in the list
	// note that we only hash the keys used in the equality comparison
	Vector hash_values(TypeId::HASH);
	Hash(keys, *current_sel, added_count, hash_values);

	// serialize the keys to the key locations
	for(idx_t i = 0; i < keys.column_count(); i++) {
		SerializeVectorData(key_data[i], keys.data[i].type, *current_sel, added_count, key_locations);
	}
	// now serialize the payload
	for(idx_t i = 0; i < payload.column_count(); i++) {
		SerializeVector(payload.data[i], payload.size(), *current_sel, added_count, key_locations);
	}
	SerializeVector(hash_values, payload.size(), *current_sel, added_count, key_locations);
}

void JoinHashTable::InsertHashes(Vector &hashes, idx_t count, data_ptr_t key_locations[]) {
	assert(hashes.type == TypeId::HASH);

	// use bitmask to get position in array
	ApplyBitmask(hashes, count);

	hashes.Normalify(count);
	assert(hashes.vector_type == VectorType::FLAT_VECTOR);
	auto pointers = (data_ptr_t *)hash_map->node->buffer;
	auto indices = FlatVector::GetData<uint64_t>(hashes);
	for(idx_t i = 0; i < count; i++) {
		auto index = indices[i];
		// set prev in current key to the value (NOTE: this will be nullptr if
		// there is none)
		auto prev_pointer = (data_ptr_t *)(key_locations[i] + tuple_size);
		*prev_pointer = pointers[index];

		// set pointer to current tuple
		pointers[index] = key_locations[i];
	}
}

void JoinHashTable::Finalize() {
	// the build has finished, now iterate over all the nodes and construct the final hash table
	// select a HT that has at least 50% empty space
	idx_t capacity = NextPowerOfTwo(std::max(count * 2, (idx_t)(Storage::BLOCK_ALLOC_SIZE / sizeof(data_ptr_t)) + 1));
	// size needs to be a power of 2
	assert((capacity & (capacity - 1)) == 0);
	bitmask = capacity - 1;

	// allocate the HT and initialize it with all-zero entries
	hash_map = buffer_manager.Allocate(capacity * sizeof(data_ptr_t));
	memset(hash_map->node->buffer, 0, capacity * sizeof(data_ptr_t));

	Vector hashes(TypeId::HASH);
	auto hash_data = FlatVector::GetData<uint64_t>(hashes);
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	// now construct the actual hash table; scan the nodes
	// as we can the nodes we pin all the blocks of the HT and keep them pinned until the HT is destroyed
	// this is so that we can keep pointers around to the blocks
	// FIXME: if we cannot keep everything pinned in memory, we could switch to an out-of-memory merge join or so
	for (auto &block : blocks) {
		auto handle = buffer_manager.Pin(block.block_id);
		data_ptr_t dataptr = handle->node->buffer;
		idx_t entry = 0;
		while (entry < block.count) {
			// fetch the next vector of entries from the blocks
			idx_t next = std::min((idx_t)STANDARD_VECTOR_SIZE, block.count - entry);
			for (idx_t i = 0; i < next; i++) {
				hash_data[i] = *((uint64_t *)(dataptr + tuple_size));
				key_locations[i] = dataptr;
				dataptr += entry_size;
			}
			// now insert into the hash table
			InsertHashes(hashes, next, key_locations);

			entry += next;
		}
		pinned_handles.push_back(move(handle));
	}
	finalized = true;
}

unique_ptr<ScanStructure> JoinHashTable::Probe(DataChunk &keys) {
	assert(count > 0); // should be handled before
	assert(finalized);

	// set up the scan structure
	auto ss = make_unique<ScanStructure>(*this);

	// first prepare the keys for probing
	const SelectionVector *current_sel;
	ss->count = PrepareKeys(keys, ss->key_data, current_sel, ss->sel_vector);
	if (ss->count == 0) {
		return ss;
	}

	// hash all the keys
	Vector hashes(TypeId::HASH);
	Hash(keys, *current_sel, ss->count, hashes);

	// now initialize the pointers of the scan structure based on the hashes
	ApplyBitmask(hashes, *current_sel, ss->count, ss->pointers);

	if (join_type != JoinType::INNER) {
		ss->found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
		memset(ss->found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
	}

	// create the selection vector linking to only non-empty entries
	idx_t count = 0;
	auto pointers = FlatVector::GetData<data_ptr_t>(ss->pointers);
	for (idx_t i = 0; i < ss->count; i++) {
		auto idx = current_sel->get_index(i);
		auto chain_pointer = (data_ptr_t *)(pointers[idx]);
		pointers[idx] = *chain_pointer;
		if (pointers[idx]) {
			ss->sel_vector.set_index(count++, idx);

		}
	}
	ss->count = count;
	return ss;
}

ScanStructure::ScanStructure(JoinHashTable &ht) : sel_vector(STANDARD_VECTOR_SIZE), ht(ht), finished(false) {
	pointers.Initialize(TypeId::POINTER);
}

void ScanStructure::Next(DataChunk &keys, DataChunk &left, DataChunk &result) {
	if (finished) {
		return;
	}

	switch (ht.join_type) {
	case JoinType::INNER:
		NextInnerJoin(keys, left, result);
		break;
	case JoinType::SEMI:
		NextSemiJoin(keys, left, result);
		break;
	case JoinType::MARK:
		NextMarkJoin(keys, left, result);
		break;
	case JoinType::ANTI:
		NextAntiJoin(keys, left, result);
		break;
	case JoinType::LEFT:
		NextLeftJoin(keys, left, result);
		break;
	case JoinType::SINGLE:
		NextSingleJoin(keys, left, result);
		break;
	default:
		throw Exception("Unhandled join type in JoinHashTable");
	}
}

template<class T, class OP>
static idx_t TemplatedGather(VectorData &vdata, Vector &pointers, const SelectionVector &current_sel, idx_t count, SelectionVector &match_sel, idx_t offset) {
	idx_t result_count = 0;
	auto data = (T*) vdata.data;
	auto ptrs = FlatVector::GetData<uint64_t>(pointers);
	for(idx_t i = 0; i < count; i++) {
		auto idx = current_sel.get_index(i);
		auto kidx = vdata.sel->get_index(idx);
		auto gdata = (T*) (ptrs[idx] + offset);
		if ((*vdata.nullmask)[kidx]) {
			if (IsNullValue<T>(*gdata)) {
				match_sel.set_index(result_count++, idx);
			}
		} else {
			if (OP::template Operation<T>(data[kidx], *gdata)) {
				match_sel.set_index(result_count++, idx);
			}
		}
	}
	return result_count;
}

template<class OP>
static idx_t GatherSwitch(VectorData &data, TypeId type, Vector &pointers, const SelectionVector &current_sel, idx_t count, SelectionVector &match_sel, idx_t offset) {
	switch(type) {
	case TypeId::INT32:
		return TemplatedGather<int32_t, OP>(data, pointers, current_sel, count, match_sel, offset);
	default:
		throw NotImplementedException("Unimplemented vector type for join");
	}

}

idx_t ScanStructure::ResolvePredicates(DataChunk &keys, SelectionVector &result) {
	SelectionVector *current_sel = &this->sel_vector;
	idx_t remaining_count = this->count;
	idx_t offset = 0;
	for (idx_t i = 0; i < ht.predicates.size(); i++) {
		switch (ht.predicates[i]) {
		case ExpressionType::COMPARE_EQUAL:
			remaining_count = GatherSwitch<Equals>(key_data[i], keys.data[i].type, this->pointers, *current_sel, remaining_count, result, offset);
			break;
		default:
			throw NotImplementedException("Unimplemented comparison type for join");
		}
		if (remaining_count == 0) {
			break;
		}
		current_sel = &result;
		offset += GetTypeIdSize(keys.data[i].type);
	}
	return remaining_count;
}

void ScanStructure::ResolvePredicates(DataChunk &keys, Vector &final_result) {
	throw NotImplementedException("FIXME");
	// // initialize result to false
	// auto result_data = FlatVector::GetData<bool>(final_result);
	// for(idx_t i = 0; i < keys.size(); i++) {
	// 	result_data[i] = false;
	// }

	// // now resolve the predicates with the keys
	// sel_t matching_tuples[STANDARD_VECTOR_SIZE];
	// idx_t match_count = ResolvePredicates(keys, matching_tuples);

	// // finished with all comparisons, mark the matching tuples
	// for (idx_t i = 0; i < match_count; i++) {
	// 	result_data[matching_tuples[i]] = true;
	// }
}

idx_t ScanStructure::ScanInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result, SelectionVector &result_vector) {
	while(true) {
		// resolve the predicates for this set of keys
		idx_t result_count = ResolvePredicates(keys, result_vector);

		// after doing all the comparisons set the found_match vector
		if (found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				auto idx = result_vector.get_index(i);
				found_match[idx] = true;
			}
		}
		if (result_count > 0) {
			return result_count;
		}
		// no matches found: check the next set of pointers
		AdvancePointers();
		if (this->count == 0) {
			return 0;
		}
	}
}

void ScanStructure::AdvancePointers() {
	// now for all the pointers, we move on to the next set of pointers
	idx_t new_count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(this->pointers);
	for(idx_t i = 0; i < this->count; i++) {
		auto idx = this->sel_vector.get_index(i);
		auto chain_pointer = (data_ptr_t *)(ptrs[idx] + ht.tuple_size);
		ptrs[idx] = *chain_pointer;
		if (ptrs[idx]) {
			this->sel_vector.set_index(new_count++, idx);
		}
	}
	this->count = new_count;
}



template<class T>
static void TemplatedGatherResult(Vector &result, uint64_t *pointers, SelectionVector &sel_vector, idx_t count, idx_t offset) {
	auto rdata = FlatVector::GetData<T>(result);
	auto &nullmask = FlatVector::Nullmask(result);
	for(idx_t i = 0; i < count; i++) {
		auto pidx = sel_vector.get_index(i);
		auto hdata = (T*) (pointers[pidx] + offset);
		if (IsNullValue<T>(*hdata)) {
			nullmask[i] = true;
		} else {
			rdata[i] = *hdata;
		}
	}
}

void ScanStructure::GatherResult(Vector &result, SelectionVector &sel_vector, idx_t count, idx_t &offset) {
	result.vector_type = VectorType::FLAT_VECTOR;
	auto ptrs = FlatVector::GetData<uint64_t>(pointers);
	switch(result.type) {
	case TypeId::INT32:
		TemplatedGatherResult<int32_t>(result, ptrs, sel_vector, count, offset);
		break;
	default:
		throw NotImplementedException("bla");
	}
	offset += GetTypeIdSize(result.type);
}

void ScanStructure::NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	assert(result.column_count() == left.column_count() + ht.build_types.size());
	if (this->count == 0) {
		// no pointers left to chase
		return;
	}

	SelectionVector result_vector(STANDARD_VECTOR_SIZE);

	idx_t result_count = ScanInnerJoin(keys, left, result, result_vector);
	if (result_count > 0) {
		// matches were found
		// construct the result
		// on the LHS, we create a slice using the result vector
		for (idx_t i = 0; i < left.column_count(); i++) {
			result.data[i].Slice(left.data[i], result_vector);
		}
		// on the RHS, we need to fetch the data from the hash table
		idx_t offset = ht.condition_size;
		for (idx_t i = 0; i < ht.build_types.size(); i++) {
			auto &vector = result.data[left.column_count() + i];
			assert(vector.type == ht.build_types[i]);
			GatherResult(vector, result_vector, result_count, offset);
		}
		result.SetCardinality(result_count);
		AdvancePointers();
	}
}

void ScanStructure::ScanKeyMatches(DataChunk &keys) {
	throw NotImplementedException("FIXME");
	// // the semi-join, anti-join and mark-join we handle a differently from the inner join
	// // since there can be at most STANDARD_VECTOR_SIZE results
	// // we handle the entire chunk in one call to Next().
	// // for every pointer, we keep chasing pointers and doing comparisons.
	// // this results in a boolean array indicating whether or not the tuple has a match
	// Vector comparison_result(pointers.cardinality(), TypeId::BOOL);
	// while (pointers.size() > 0) {
	// 	// resolve the predicates for the current set of pointers
	// 	ResolvePredicates(keys, comparison_result);

	// 	// after doing all the comparisons we loop to find all the matches
	// 	auto ptrs = (data_ptr_t *)pointers.GetData();
	// 	idx_t new_count = 0;
	// 	VectorOperations::ExecType<bool>(comparison_result, [&](bool match, idx_t index, idx_t k) {
	// 		if (match) {
	// 			// found a match, set the entry to true
	// 			// after this we no longer need to check this entry
	// 			found_match[index] = true;
	// 		} else {
	// 			// did not find a match, keep on looking for this entry
	// 			// first check if there is a next entry
	// 			auto prev_pointer = (data_ptr_t *)(ptrs[index] + ht.build_size);
	// 			ptrs[index] = *prev_pointer;
	// 			if (ptrs[index]) {
	// 				// if there is a next pointer, we keep this entry
	// 				sel_vector[new_count++] = index;
	// 			}
	// 		}
	// 	});
	// 	pointers.SetCount(new_count);
	// }
}

template <bool MATCH> void ScanStructure::NextSemiOrAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	throw NotImplementedException("FIXME");
	// assert(left.column_count() == result.column_count());
	// assert(keys.size() == left.size());
	// // create the selection vector from the matches that were found
	// idx_t result_count = 0;
	// for (idx_t i = 0; i < keys.size(); i++) {
	// 	if (found_match[i] == MATCH) {
	// 		// part of the result
	// 		result.owned_sel_vector[result_count++] = i;
	// 	}
	// }
	// // construct the final result
	// if (result_count > 0) {
	// 	// we only return the columns on the left side
	// 	// reference the columns of the left side from the result
	// 	for (idx_t i = 0; i < left.column_count(); i++) {
	// 		result.data[i].Reference(left.data[i]);
	// 	}
	// 	// project them using the result selection vector
	// 	result.SetCardinality(result_count, result.owned_sel_vector);
	// } else {
	// 	assert(result.size() == 0);
	// }
}

void ScanStructure::NextSemiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// first scan for key matches
	ScanKeyMatches(keys);
	// then construct the result from all tuples with a match
	NextSemiOrAntiJoin<true>(keys, left, result);

	finished = true;
}

void ScanStructure::NextAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// first scan for key matches
	ScanKeyMatches(keys);
	// then construct the result from all tuples that did not find a match
	NextSemiOrAntiJoin<false>(keys, left, result);

	finished = true;
}

void ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &child, DataChunk &result, bool found_match[],
                             bool right_has_null) {
	throw NotImplementedException("FIXME");
	// // for the initial set of columns we just reference the left side
	// result.SetCardinality(child);
	// for (idx_t i = 0; i < child.column_count(); i++) {
	// 	result.data[i].Reference(child.data[i]);
	// }
	// auto &result_vector = result.data.back();
	// // first we set the NULL values from the join keys
	// // if there is any NULL in the keys, the result is NULL
	// if (join_keys.column_count() > 0) {
	// 	result_vector.nullmask = join_keys.data[0].nullmask;
	// 	for (idx_t i = 1; i < join_keys.column_count(); i++) {
	// 		result_vector.nullmask |= join_keys.data[i].nullmask;
	// 	}
	// }
	// // now set the remaining entries to either true or false based on whether a match was found
	// auto bool_result = (bool *)result_vector.GetData();
	// for (idx_t i = 0; i < child.size(); i++) {
	// 	bool_result[i] = found_match[i];
	// }
	// // if the right side contains NULL values, the result of any FALSE becomes NULL
	// if (right_has_null) {
	// 	for (idx_t i = 0; i < child.size(); i++) {
	// 		if (!bool_result[i]) {
	// 			result_vector.nullmask[i] = true;
	// 		}
	// 	}
	// }
}

void ScanStructure::NextMarkJoin(DataChunk &keys, DataChunk &input, DataChunk &result) {
	throw NotImplementedException("FIXME");
	// assert(result.column_count() == input.column_count() + 1);
	// assert(result.data.back().type == TypeId::BOOL);
	// assert(!input.sel_vector);
	// // this method should only be called for a non-empty HT
	// assert(ht.count > 0);

	// ScanKeyMatches(keys);
	// if (ht.correlated_mark_join_info.correlated_types.size() == 0) {
	// 	ConstructMarkJoinResult(keys, input, result, found_match, ht.has_null);
	// } else {
	// 	auto &info = ht.correlated_mark_join_info;
	// 	// there are correlated columns
	// 	// first we fetch the counts from the aggregate hashtable corresponding to these entries
	// 	assert(keys.column_count() == info.group_chunk.column_count() + 1);
	// 	info.group_chunk.SetCardinality(keys);
	// 	for (idx_t i = 0; i < info.group_chunk.column_count(); i++) {
	// 		info.group_chunk.data[i].Reference(keys.data[i]);
	// 	}
	// 	info.correlated_counts->FetchAggregates(info.group_chunk, info.result_chunk);
	// 	assert(!info.result_chunk.sel_vector);

	// 	// for the initial set of columns we just reference the left side
	// 	result.SetCardinality(input);
	// 	for (idx_t i = 0; i < input.column_count(); i++) {
	// 		result.data[i].Reference(input.data[i]);
	// 	}
	// 	// create the result matching vector
	// 	auto &result_vector = result.data.back();
	// 	// first set the nullmask based on whether or not there were NULL values in the join key
	// 	result_vector.nullmask = keys.data.back().nullmask;

	// 	auto bool_result = (bool *)result_vector.GetData();
	// 	auto count_star = (int64_t *)info.result_chunk.data[0].GetData();
	// 	auto count = (int64_t *)info.result_chunk.data[1].GetData();
	// 	// set the entries to either true or false based on whether a match was found
	// 	for (idx_t i = 0; i < input.size(); i++) {
	// 		assert(count_star[i] >= count[i]);
	// 		bool_result[i] = found_match[i];
	// 		if (!bool_result[i] && count_star[i] > count[i]) {
	// 			// RHS has NULL value and result is false:, set to null
	// 			result_vector.nullmask[i] = true;
	// 		}
	// 		if (count_star[i] == 0) {
	// 			// count == 0, set nullmask to false (we know the result is false now)
	// 			result_vector.nullmask[i] = false;
	// 		}
	// 	}
	// }
	// finished = true;
}

void ScanStructure::NextLeftJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	throw NotImplementedException("FIXME");
	// // a LEFT OUTER JOIN is identical to an INNER JOIN except all tuples that do
	// // not have a match must return at least one tuple (with the right side set
	// // to NULL in every column)
	// NextInnerJoin(keys, left, result);
	// if (result.size() == 0) {
	// 	// no entries left from the normal join
	// 	// fill in the result of the remaining left tuples
	// 	// together with NULL values on the right-hand side
	// 	idx_t remaining_count = 0;
	// 	for (idx_t i = 0; i < left.size(); i++) {
	// 		if (!found_match[i]) {
	// 			result.owned_sel_vector[remaining_count++] = i;
	// 		}
	// 	}
	// 	if (remaining_count > 0) {
	// 		// have remaining tuples
	// 		// first set the left side
	// 		idx_t i = 0;
	// 		for (; i < left.column_count(); i++) {
	// 			result.data[i].Reference(left.data[i]);
	// 		}
	// 		// now set the right side to NULL
	// 		for (; i < result.column_count(); i++) {
	// 			result.data[i].vector_type = VectorType::CONSTANT_VECTOR;
	// 			result.data[i].nullmask[0] = true;
	// 		}
	// 		result.SetCardinality(remaining_count, result.owned_sel_vector);
	// 	}
	// 	finished = true;
	// }
}

void ScanStructure::NextSingleJoin(DataChunk &keys, DataChunk &input, DataChunk &result) {
	throw NotImplementedException("FIXME");
	// // single join
	// // this join is similar to the semi join except that
	// // (1) we actually return data from the RHS and
	// // (2) we return NULL for that data if there is no match
	// Vector comparison_result(pointers.cardinality(), TypeId::BOOL);

	// auto build_pointers = (data_ptr_t *)build_pointer_vector.GetData();
	// idx_t result_count = 0;
	// sel_t result_sel_vector[STANDARD_VECTOR_SIZE];
	// while (pointers.size() > 0) {
	// 	// resolve the predicates for all the pointers
	// 	ResolvePredicates(keys, comparison_result);

	// 	auto ptrs = (data_ptr_t *)pointers.GetData();
	// 	// after doing all the comparisons we loop to find all the actual matches
	// 	idx_t new_count = 0;
	// 	auto psel = pointers.sel_vector();
	// 	VectorOperations::ExecType<bool>(comparison_result, [&](bool match, idx_t index, idx_t k) {
	// 		if (match) {
	// 			// found a match for this index
	// 			// set the build_pointers to this position
	// 			found_match[index] = true;
	// 			build_pointers[result_count] = ptrs[index];
	// 			result_sel_vector[result_count] = index;
	// 			result_count++;
	// 		} else {
	// 			auto prev_pointer = (data_ptr_t *)(ptrs[index] + ht.build_size);
	// 			ptrs[index] = *prev_pointer;
	// 			if (ptrs[index]) {
	// 				// if there is a next pointer, and we have not found a match yet, we keep this entry
	// 				psel[new_count++] = index;
	// 			}
	// 		}
	// 	});
	// 	pointers.SetCount(new_count);
	// }

	// // now we construct the final result
	// build_pointer_vector.SetCount(result_count);
	// // reference the columns of the left side from the result
	// assert(input.column_count() > 0);
	// result.SetCardinality(result_count, result_sel_vector);
	// for (idx_t i = 0; i < input.column_count(); i++) {
	// 	result.data[i].Reference(input.data[i]);
	// }
	// // now fetch the data from the RHS
	// for (idx_t i = 0; i < ht.build_types.size(); i++) {
	// 	auto &vector = result.data[input.column_count() + i];
	// 	// set NULL entries for every entry that was not found
	// 	vector.nullmask.set();
	// 	for (idx_t j = 0; j < result_count; j++) {
	// 		vector.nullmask[result_sel_vector[j]] = false;
	// 	}
	// 	// fetch the data from the HT for tuples that found a match
	// 	VectorOperations::Gather::Set(build_pointer_vector, vector);
	// 	VectorOperations::AddInPlace(build_pointer_vector, GetTypeIdSize(ht.build_types[i]));
	// }
	// result.SetCardinality(input);
	// // like the SEMI, ANTI and MARK join types, the SINGLE join only ever does one pass over the HT per input chunk
	// finished = true;
}

} // namespace duckdb
