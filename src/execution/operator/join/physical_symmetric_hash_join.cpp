#include "duckdb/execution/operator/join/physical_symmetric_hash_join.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include<iostream>
using namespace duckdb;
using namespace std;

class PhysicalSymmetricHashJoinState : public PhysicalComparisonJoinState {
public:
	PhysicalSymmetricHashJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
	    : PhysicalComparisonJoinState(left, right, conditions), initialized(false), side(false) {
	}

	bool initialized;
	DataChunk cached_chunk;
	DataChunk join_keys;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
  bool side;
};

PhysicalSymmetricHashJoin::PhysicalSymmetricHashJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   vector<index_t> left_projection_map, vector<index_t> right_projection_map)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type),
      right_projection_map(right_projection_map) {
	children.push_back(move(left));
	children.push_back(move(right));

	assert(left_projection_map.size() == 0);

	hash_table_left =
	    make_unique<JoinHashTable>(*context.db.storage->buffer_manager, conditions,
	                               LogicalOperator::MapTypes(children[0]->GetTypes(), left_projection_map), type);

  hash_table_right =
      make_unique<JoinHashTable>(*context.db.storage->buffer_manager, conditions,
                               	 LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map), type);
}

PhysicalSymmetricHashJoin::PhysicalSymmetricHashJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type)
    : PhysicalSymmetricHashJoin(context, op, move(left), move(right), move(cond), join_type, {}, {}) {
}

void PhysicalSymmetricHashJoin::BuildHashTable(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_, int child) {
	auto state = reinterpret_cast<PhysicalSymmetricHashJoinState *>(state_);
  //chunk.Print();
	// build the HT
	//auto right_state = children[1]->GetOperatorState();
//	auto types = children[1]->GetTypes();

  if(child == 0){
    DataChunk build_chunk;

    if (right_projection_map.size() > 0) {
      build_chunk.Initialize(hash_table_left->build_types);
    }

    state->join_keys.Initialize(hash_table_left->condition_types);
	//while (true) {
		// get the child chunk
		//children[1]->GetChunk(context, right_chunk, right_state.get());
		if (chunk.size() == 0) {
			return;
		}
		// resolve the join keys for the right chunk
		state->lhs_executor.Execute(chunk, state->join_keys);
		// build the HT
		if (right_projection_map.size() > 0) {
			// there is a projection map: fill the build chunk with the projected columns
			build_chunk.Reset();
			build_chunk.SetCardinality(chunk);
			for (index_t i = 0; i < right_projection_map.size(); i++) {
				build_chunk.data[i].Reference(chunk.data[right_projection_map[i]]);
			}
			hash_table_left->Build(state->join_keys, build_chunk);
		} else {
			// there is not a projected map: place the entire right chunk in the HT
			hash_table_left->Build(state->join_keys, chunk);
		}
		if (hash_table_left->size() == 0 &&
					(hash_table_left->join_type == JoinType::INNER || hash_table_left->join_type == JoinType::SEMI)) {
				// empty hash table with INNER or SEMI join means empty result set
				return;
		}
		
		hash_table_left->Finalize();

  } else if (child == 1){

    DataChunk build_chunk;

    if (right_projection_map.size() > 0) {
      build_chunk.Initialize(hash_table_right->build_types);
  	}

  	state->join_keys.Initialize(hash_table_right->condition_types);
  	//while (true) {
  		// get the child chunk
  		//children[1]->GetChunk(context, right_chunk, right_state.get());
    if (chunk.size() == 0) {
      return;
    }
    // resolve the join keys for the right chunk
    state->rhs_executor.Execute(chunk, state->join_keys);
    // build the HT
    if (right_projection_map.size() > 0) {
      // there is a projection map: fill the build chunk with the projected columns
      build_chunk.Reset();
      build_chunk.SetCardinality(chunk);
      for (index_t i = 0; i < right_projection_map.size(); i++) {
        build_chunk.data[i].Reference(chunk.data[right_projection_map[i]]);
      }
      hash_table_right->Build(state->join_keys, build_chunk);
    } else {
      // there is not a projected map: place the entire right chunk in the HT
      hash_table_right->Build(state->join_keys, chunk);
    }

    if (hash_table_right->size() == 0 &&
    		    (hash_table_right->join_type == JoinType::INNER || hash_table_right->join_type == JoinType::SEMI)) {
    			// empty hash table with INNER or SEMI join means empty result set
      return;
    }
    hash_table_right->Finalize();
  }
	//}

}

void PhysicalSymmetricHashJoin::ProbeHashTable(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_, int child) {
  auto state = reinterpret_cast<PhysicalSymmetricHashJoinState *>(state_);

  if (state->child_chunk.size() > 0 && state->scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
    state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
    if (chunk.size() > 0) {
		  return;
		}
		state->scan_structure = nullptr;
	}

	// probe the HT
  if(child == 0) {
    do {
		// fetch the chunk from the right side
		  children[1]->GetChunk(context, state->child_chunk, state->child_state.get());
  
		  if (state->child_chunk.size() == 0) {
        return;
		  }
		// remove any selection vectors
		  state->child_chunk.ClearSelectionVector();

      if (hash_table_left->size() == 0) {
  			// empty hash table, special case
  			if (hash_table_left->join_type == JoinType::ANTI) {
  				// anti join with empty hash table, NOP join
  				// return the input
  				assert(chunk.column_count() == state->child_chunk.column_count());
  				chunk.Reference(state->child_chunk);
  				return;
  			} else if (hash_table_left->join_type == JoinType::MARK) {
  				// MARK join with empty hash table
  				assert(hash_table_left->join_type == JoinType::MARK);
  				assert(chunk.column_count() == state->child_chunk.column_count() + 1);
  				auto &result_vector = chunk.data.back();
  				assert(result_vector.type == TypeId::BOOL);
  				// for every data vector, we just reference the child chunk
  				chunk.SetCardinality(state->child_chunk);
  				for (index_t i = 0; i < state->child_chunk.column_count(); i++) {
  					chunk.data[i].Reference(state->child_chunk.data[i]);
  				}
  				// for the MARK vector:
  				// if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
  				// entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
  				// has NULL for every input entry
  				if (!hash_table_left->has_null) {
  					auto bool_result = (bool *)result_vector.GetData();
  					for (index_t i = 0; i < result_vector.size(); i++) {
  						bool_result[i] = false;
  					}
  				} else {
  					result_vector.nullmask.set();
  				}
  				return;
  			} else if (hash_table_left->join_type == JoinType::LEFT || hash_table_left->join_type == JoinType::OUTER ||
  			           hash_table_left->join_type == JoinType::SINGLE) {
  				// LEFT/FULL OUTER/SINGLE join and build side is empty
  				// for the LHS we reference the data
  				chunk.SetCardinality(state->child_chunk.size());
  				for (index_t i = 0; i < state->child_chunk.column_count(); i++) {
  					chunk.data[i].Reference(state->child_chunk.data[i]);
  				}
  				// for the RHS
  				for (index_t k = state->child_chunk.column_count(); k < chunk.column_count(); k++) {
  					chunk.data[k].vector_type = VectorType::CONSTANT_VECTOR;
  					chunk.data[k].nullmask[0] = true;
  				}
  				return;
  			}
  		}

  		// resolve the join keys for the right chunk
  		state->rhs_executor.Execute(state->child_chunk, state->join_keys);

  		// perform the actual probe
  		state->scan_structure = hash_table_left->Probe(state->join_keys);

  		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
  	} while (chunk.size() == 0);

  } else if(child == 1){
    do {
    // fetch the chunk from the left side
    	children[0]->GetChunk(context, state->child_chunk, state->child_state.get());

    	if (state->child_chunk.size() == 0) {
    		return;
    	}
    	// remove any selection vectors
    	state->child_chunk.ClearSelectionVector();
    	if (hash_table_right->size() == 0) {
    		// empty hash table, special case
    		if (hash_table_right->join_type == JoinType::ANTI) {
    			// anti join with empty hash table, NOP join
    			// return the input
    			assert(chunk.column_count() == state->child_chunk.column_count());
    			chunk.Reference(state->child_chunk);
    			return;
    		} else if (hash_table_right->join_type == JoinType::MARK) {
    			// MARK join with empty hash table
    			assert(hash_table_right->join_type == JoinType::MARK);
    			assert(chunk.column_count() == state->child_chunk.column_count() + 1);
    			auto &result_vector = chunk.data.back();
    			assert(result_vector.type == TypeId::BOOL);
    			// for every data vector, we just reference the child chunk
    			chunk.SetCardinality(state->child_chunk);
    			for (index_t i = 0; i < state->child_chunk.column_count(); i++) {
    				chunk.data[i].Reference(state->child_chunk.data[i]);
    			}
    			// for the MARK vector:
    			// if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
    			// entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
    			// has NULL for every input entry
    			if (!hash_table_right->has_null) {
    				auto bool_result = (bool *)result_vector.GetData();
    				for (index_t i = 0; i < result_vector.size(); i++) {
    					bool_result[i] = false;
    				}
    			} else {
    				result_vector.nullmask.set();
    			}
    			return;
    		} else if (hash_table_right->join_type == JoinType::LEFT || hash_table_right->join_type == JoinType::OUTER ||
    			           hash_table_right->join_type == JoinType::SINGLE) {
    			// LEFT/FULL OUTER/SINGLE join and build side is empty
    			// for the LHS we reference the data
    			chunk.SetCardinality(state->child_chunk.size());
    			for (index_t i = 0; i < state->child_chunk.column_count(); i++) {
    				chunk.data[i].Reference(state->child_chunk.data[i]);
    			}
    			// for the RHS
    			for (index_t k = state->child_chunk.column_count(); k < chunk.column_count(); k++) {
    				chunk.data[k].vector_type = VectorType::CONSTANT_VECTOR;
    				chunk.data[k].nullmask[0] = true;
    			}
    			return;
    		}
    	}

    	// resolve the join keys for the left chunk
    	state->lhs_executor.Execute(state->child_chunk, state->join_keys);


    	// perform the actual probe
    	state->scan_structure = hash_table_right->Probe(state->join_keys);


      state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);

    } while (chunk.size() == 0);
  }
}

unique_ptr<PhysicalOperatorState> PhysicalSymmetricHashJoin::GetOperatorState() {
	return make_unique<PhysicalSymmetricHashJoinState>(children[0].get(), children[1].get(), conditions);
}

void PhysicalSymmetricHashJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalSymmetricHashJoinState *>(state_);


	state->cached_chunk.Initialize(types);

	auto right_state = children[1]->GetOperatorState();
  auto right_types = children[1]->GetTypes();

  DataChunk right_chunk;
  right_chunk.Initialize(right_types);
  children[1]->GetChunk(context, right_chunk,right_state.get());

  BuildHashTable(context, right_chunk, state_, 1);

	if (hash_table_right->size() == 0 &&
			 (hash_table_right->join_type == JoinType::INNER || hash_table_right->join_type == JoinType::SEMI)) {
		// empty hash table with INNER or SEMI join means empty result set
    return;
	}
  

	auto left_state = children[0]->GetOperatorState();
  auto left_types = children[0]->GetTypes();

  DataChunk left_chunk;
  left_chunk.Initialize(left_types);
  children[0]->GetChunk(context, left_chunk, left_state.get());
  BuildHashTable(context, left_chunk, state_, 0);

  if (hash_table_left->size() == 0 &&
  			 (hash_table_left->join_type == JoinType::INNER || hash_table_left->join_type == JoinType::SEMI)) {
  	// empty hash table with INNER or SEMI join means empty result set
  	return;
  }

 	
  ProbeHashTable(context, chunk, state, 1);
  ProbeHashTable(context, chunk, state, 0);
  



  /*#if STANDARD_VECTOR_SIZE >= 128
    if (chunk.size() == 0) {
    	if (state->cached_chunk.size() > 0) {
    		// finished probing but cached data remains, return cached chunk
    		chunk.Reference(state->cached_chunk);
    		state->cached_chunk.Reset();
    	}
    	return;
    } else if (chunk.size() < 64) {
    	// small chunk: add it to chunk cache and continue
    	state->cached_chunk.Append(chunk);
    	if (state->cached_chunk.size() >= (STANDARD_VECTOR_SIZE - 64)) {
    		// chunk cache full: return it
        chunk.Reference(state->cached_chunk);
    		state->cached_chunk.Reset();
    		return;
    	}
			else {
    		// chunk cache not full: probe again
    		chunk.Reset();
    	}
    } else {
    	return;
    }
  #else
    return;
  #endif*/



//  }
	//}
/*	do {
		ProbeHashTable(context, chunk, state); */
/*#if STANDARD_VECTOR_SIZE >= 128
		if (chunk.size() == 0) {
			if (state->cached_chunk.size() > 0) {
				// finished probing but cached data remains, return cached chunk
				chunk.Reference(state->cached_chunk);
				state->cached_chunk.Reset();
			}
			return;
		} else if (chunk.size() < 64) {
			// small chunk: add it to chunk cache and continue
			state->cached_chunk.Append(chunk);
			if (state->cached_chunk.size() >= (STANDARD_VECTOR_SIZE - 64)) {
				// chunk cache full: return it
				chunk.Reference(state->cached_chunk);
				state->cached_chunk.Reset();
				return;
			} else {
				// chunk cache not full: probe again
				chunk.Reset();
			}
		} else {
			return;
		}
#else
		return;
#endif
	//} while (true);*/
}
