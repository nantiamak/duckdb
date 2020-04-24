#include "duckdb/execution/operator/join/physical_symmetric_hash_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

class PhysicalSymmetricHashJoinState : public PhysicalComparisonJoinState {
public:
  PhysicalSymmetricHashJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
      : PhysicalComparisonJoinState(left, right, conditions), initialized(false) {
  }

  bool initialized;
  DataChunk join_keys;
  DataChunk child_chunks[2];
  unique_ptr<JoinHashTable::ScanStructure> scan_structure;
  unique_ptr<PhysicalOperatorState> child_states[2];
  int count_left=0;
  int count_right=0;
};

PhysicalSymmetricHashJoin::PhysicalSymmetricHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type) {
//  hash_table = make_unique<JoinHashTable>(conditions, right->GetTypes(), join_type);
  hash_tables[0]= make_unique<JoinHashTable>(conditions, left->GetTypes(), join_type);
  hash_tables[1]= make_unique<JoinHashTable>(conditions, right->GetTypes(), join_type);


  children.push_back(move(left));
  children.push_back(move(right));

}

void PhysicalSymmetricHashJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {


  int child=1;
  auto state = reinterpret_cast<PhysicalSymmetricHashJoinState *>(state_);

  if (state->child_chunks[1-child].size() > 0 && state->scan_structure) {
  // still have elements remaining from the previous probe (i.e. we got
  // >1024 elements in the previous probe)
    state->scan_structure->NextSymmetric(state->join_keys, state->child_chunks[1-child], 1-child, chunk);

    if (chunk.size() > 0) {
      return;
    }
    state->scan_structure = nullptr;
  }

  if (state->child_chunks[child].size() > 0 && state->scan_structure) {
  // still have elements remaining from the previous probe (i.e. we got
  // >1024 elements in the previous probe)
    state->scan_structure->NextSymmetric(state->join_keys, state->child_chunks[child], child, chunk);

    if (chunk.size() > 0) {
      return;
    }
    state->scan_structure = nullptr;
  }

  state->child_chunks[0].Initialize(children[0]->GetTypes());
  state->child_chunks[1].Initialize(children[1]->GetTypes());


  do{

    children[child]->GetChunk(context, state->child_chunks[child], state->child_states[child].get());

    state->join_keys.Initialize(hash_tables[child]->condition_types);
    if(state->child_chunks[child].size() == 0) {
      child=1-child;
      children[child]->GetChunk(context, state->child_chunks[child], state->child_states[child].get());

      if(state->child_chunks[child].size() == 0) {
        return;
      }
    }

    if(child==1){
     state->rhs_executor.Execute(state->child_chunks[child], state->join_keys);
   } else {
     state->lhs_executor.Execute(state->child_chunks[child], state->join_keys);
   }


   // build the HT
   hash_tables[child]->Build(state->join_keys, state->child_chunks[child]);


   if (hash_tables[child]->size() == 0 &&
     (hash_tables[child]->join_type == JoinType::INNER || hash_tables[child]->join_type == JoinType::SEMI)) {
   // empty hash table with INNER or SEMI join means empty result set
        return;
    }


   // remove any selection vectors
   state->child_chunks[child].Flatten();
   if (hash_tables[1-child]->size() == 0) {
     // empty hash table, special case
     if (hash_tables[1-child]->join_type == JoinType::ANTI) {
       // anti join with empty hash table, NOP join
       // return the input
       assert(chunk.column_count == state->child_chunks[child].column_count);
       for (index_t i = 0; i < chunk.column_count; i++) {
         chunk.data[i].Reference(state->child_chunks[child].data[i]);
       }
       return;
     } else if (hash_tables[1-child]->join_type == JoinType::MARK) {

       // MARK join with empty hash table
       assert(hash_tables[1-child]->join_type == JoinType::MARK);
       assert(chunk.column_count == state->child_chunks[child].column_count + 1);
       auto &result_vector = chunk.data[state->child_chunks[child].column_count];
       assert(result_vector.type == TypeId::BOOLEAN);
       result_vector.count = state->child_chunks[child].size();
       // for every data vector, we just reference the child chunk
       for (index_t i = 0; i < state->child_chunks[child].column_count; i++) {
         chunk.data[i].Reference(state->child_chunks[child].data[i]);
       }
       // for the MARK vector:
       // if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
       // entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
       // has NULL for every input entry
       if (!hash_tables[1-child]->has_null) {
         auto bool_result = (bool *)result_vector.data;
         for (index_t i = 0; i < result_vector.count; i++) {
           bool_result[i] = false;
         }
       } else {
         result_vector.nullmask.set();
       }
       return;
     }
   }

   if(child==1){
    state->rhs_executor.Execute(state->child_chunks[child], state->join_keys);
  } else {
    state->lhs_executor.Execute(state->child_chunks[child], state->join_keys);
  }


   // perform the actual probe
   state->scan_structure = hash_tables[1-child]->Probe(state->join_keys);
   state->scan_structure->NextSymmetric(state->join_keys, state->child_chunks[child], child, chunk);

   child=1-child;


 }while(chunk.size()==0);


}

unique_ptr<PhysicalOperatorState> PhysicalSymmetricHashJoin::GetOperatorState() {
  auto state = make_unique<PhysicalSymmetricHashJoinState>(children[0].get(), children[1].get(), conditions);
  state->child_states[0]=children[0]->GetOperatorState();
  state->child_states[1]=children[1]->GetOperatorState();
  return(move(state));
}
