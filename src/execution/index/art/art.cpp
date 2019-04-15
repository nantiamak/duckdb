#include "execution/index/art/art.hpp"
#include "common/types/static_vector.hpp"

using namespace duckdb;

ART::ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types, vector<TypeId> expression_types,
            vector<unique_ptr<Expression>> expressions,vector<unique_ptr<Expression>> unbound_expressions)
        : Index(IndexType::ART,move(expressions),move(unbound_expressions)), table(table), column_ids(column_ids), types(types) {
    tree = NULL;
    expression_result.Initialize(expression_types);
    int n = 1;
    // little endian if true
    if (*(char *) &n == 1) {
        is_little_endian = true;


    }
}




//TODO: Suppport  FLOAT = float32_t DOUBLE =  float64_t ,VARCHAR = 9, char, representing a null-terminated UTF-8 string
void ART::Insert(DataChunk &input, Vector &row_ids) {
    if (input.column_count > 1) {
        throw NotImplementedException("We only support single dimensional indexes currently");
    }
    assert(row_ids.type == TypeId::POINTER);
    assert(input.size() == row_ids.count);
    assert(types[0] == input.data[0].type);
    switch (input.data[0].type) {
        case TypeId:: BOOLEAN:
        case TypeId:: TINYINT:
            templated_insert<int8_t,uint8_t>(input, row_ids);
            break;
        case TypeId:: SMALLINT:
            templated_insert<int16_t,uint16_t>(input, row_ids);
            break;
        case TypeId::INTEGER:
            templated_insert<int32_t,uint32_t>(input, row_ids);
            break;
        case TypeId::BIGINT:
            templated_insert<int64_t,uint64_t>(input, row_ids);
            break;
        default:
            throw InvalidTypeException(input.data[0].type, "Invalid type for index");
    }
}

unique_ptr<IndexScanState> ART::InitializeScanSinglePredicate(Transaction &transaction,
                                                                     vector<column_t> column_ids, Value value,
                                                                     ExpressionType expression_type) {
    auto result = make_unique<ARTIndexScanState>(column_ids);
//     search inside the index for the constant value
    if (expression_type == ExpressionType::COMPARE_EQUAL) {
        result->value_left = value;
        result->expression_type_left = ExpressionType::COMPARE_EQUAL;
    }
//    else if (expression_type == ExpressionType::COMPARE_GREATERTHAN) {
//        result->current_index = SearchGT(value);
//        result->final_index = count;
//    } else if (expression_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
//        result->current_index = SearchGTE(value);
//        result->final_index = count;
//    } else if (expression_type == ExpressionType::COMPARE_LESSTHAN) {
//        result->current_index = 0;
//        result->final_index = SearchLT(value);
//    } else if (expression_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
//        result->current_index = 0;
//        result->final_index = SearchLTE(value);
//    }
    return move(result);
}

//template <class T> static size_t templated_scan(size_t &from, size_t &to, uint8_t *data, uint64_t *result_ids) {
//    auto array = (SortChunk<T> *)data;
//    size_t result_count = 0;
//    for (; from < to; from++) {
//        result_ids[result_count++] = array[from].row_id;
//        if (result_count == STANDARD_VECTOR_SIZE) {
//            from++;
//            break;
//        }
//    }
//    return result_count;
//}
//
//void ART::Scan(size_t &position_from, size_t &position_to, Value value, Vector &result_identifiers) {
//    assert(result_identifiers.type == TypeId::POINTER);
//    auto row_ids = (uint64_t *)result_identifiers.data;
//    // perform the templated scan to find the tuples to extract
//    switch (types[0]) {
//        case TypeId::TINYINT:
//            result_identifiers.count = templated_scan<int8_t>(position_from, position_to, data.get(), row_ids);
//            break;
//        case TypeId::SMALLINT:
//            result_identifiers.count = templated_scan<int16_t>(position_from, position_to, data.get(), row_ids);
//            break;
//        case TypeId::INTEGER:
//            result_identifiers.count = templated_scan<int32_t>(position_from, position_to, data.get(), row_ids);
//            break;
//        case TypeId::BIGINT:
//            result_identifiers.count = templated_scan<int64_t>(position_from, position_to, data.get(), row_ids);
//            break;
//        default:
//            throw NotImplementedException("Unimplemented type for index scan");
//    }
//}

//void OrderIndex::Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) {
//    auto state = (OrderIndexScanState *)ss;
//    // scan the index
//    StaticVector<uint64_t> result_identifiers;
//    do {
//        Scan(state->current_index, state->final_index, state->value, result_identifiers);
//        if (result_identifiers.count == 0) {
//            return;
//        }
//        // now go to the base table to fetch the tuples
//        table.Fetch(transaction, result, state->column_ids, result_identifiers);
//    } while (result_identifiers.count == 0);
//}

void ART::Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) {
    auto state = (ARTIndexScanState *)ss;
    // scan the index
    StaticVector<uint64_t> result_identifiers;
    auto row_ids = (uint64_t *)result_identifiers.data;
    assert(state->value_left.type == types[0]);
    switch (types[0]) {
        case TypeId:: BOOLEAN:
            result_identifiers.count = templated_lookup<int8_t,uint8_t>(state->value_left.value_.boolean, row_ids);
        case TypeId:: TINYINT:
            result_identifiers.count = templated_lookup<int8_t,uint8_t>(state->value_left.value_.tinyint, row_ids);
            break;
        case TypeId:: SMALLINT:
            result_identifiers.count = templated_lookup<int16_t,uint16_t>(state->value_left.value_.smallint, row_ids);
            break;
        case TypeId::INTEGER:
            result_identifiers.count = templated_lookup<int32_t,uint32_t>(state->value_left.value_.integer, row_ids);
            break;
        case TypeId::BIGINT:
            result_identifiers.count = templated_lookup<int64_t,uint64_t>(state->value_left.value_.bigint, row_ids);
            break;
    }
    table.Fetch(transaction, result, state->column_ids, result_identifiers);
}