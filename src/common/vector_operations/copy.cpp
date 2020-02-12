//===--------------------------------------------------------------------===//
// copy.cpp
// Description: This file contains the implementation of the different copy
// functions
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T>
static void copy_function(T *__restrict source, T *__restrict target, index_t offset, index_t count,
                          sel_t *__restrict sel_vector) {
	VectorOperations::Exec(
	    sel_vector, count + offset, [&](index_t i, index_t k) { target[k - offset] = source[i]; }, offset);
}

template <class T>
static void copy_function_set_null(T *__restrict source, T *__restrict target, index_t offset, index_t count,
                                   sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	if (nullmask.any()) {
		// null values, have to check the NULL values in the mask
		VectorOperations::Exec(
		    sel_vector, count + offset,
		    [&](index_t i, index_t k) {
			    if (nullmask[i]) {
				    target[k - offset] = NullValue<T>();
			    } else {
				    target[k - offset] = source[i];
			    }
		    },
		    offset);
	} else {
		// no NULL values, use normal copy
		copy_function(source, target, offset, count, sel_vector);
	}
}

template <class T, bool SET_NULL>
static void copy_loop(Vector &input, void *target, index_t offset, index_t element_count) {
	auto ldata = (T *)input.GetData();
	auto result_data = (T *)target;
	if (SET_NULL) {
		copy_function_set_null(ldata, result_data, offset, element_count, input.sel_vector(), input.nullmask);
	} else {
		copy_function(ldata, result_data, offset, element_count, input.sel_vector());
	}
}

template <bool SET_NULL> void generic_copy_loop(Vector &source, void *target, index_t offset, index_t element_count) {
	if (source.size() == 0)
		return;
	if (element_count == 0) {
		element_count = source.size();
	}
	assert(offset + element_count <= source.size());

	switch (source.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		copy_loop<int8_t, SET_NULL>(source, target, offset, element_count);
		break;
	case TypeId::INT16:
		copy_loop<int16_t, SET_NULL>(source, target, offset, element_count);
		break;
	case TypeId::INT32:
		copy_loop<int32_t, SET_NULL>(source, target, offset, element_count);
		break;
	case TypeId::INT64:
		copy_loop<int64_t, SET_NULL>(source, target, offset, element_count);
		break;
	case TypeId::HASH:
		copy_loop<uint64_t, SET_NULL>(source, target, offset, element_count);
		break;
	case TypeId::POINTER:
		copy_loop<uintptr_t, SET_NULL>(source, target, offset, element_count);
		break;
	case TypeId::FLOAT:
		copy_loop<float, SET_NULL>(source, target, offset, element_count);
		break;
	case TypeId::DOUBLE:
		copy_loop<double, SET_NULL>(source, target, offset, element_count);
		break;
	case TypeId::VARCHAR:
		copy_loop<const char *, SET_NULL>(source, target, offset, element_count);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}

//===--------------------------------------------------------------------===//
// Copy data from vector
//===--------------------------------------------------------------------===//
void VectorOperations::Copy(Vector &source, void *target, index_t offset, index_t element_count) {
	if (!TypeIsConstantSize(source.type)) {
		throw InvalidTypeException(source.type, "Cannot copy non-constant size types using this method!");
	}
	generic_copy_loop<false>(source, target, offset, element_count);
}

void VectorOperations::CopyToStorage(Vector &source, void *target, index_t offset, index_t element_count) {
	generic_copy_loop<true>(source, target, offset, element_count);
}

void VectorOperations::Copy(Vector &source, Vector &target, const VectorCardinality &cardinality, index_t offset) {
	if (source.type != target.type) {
		throw TypeMismatchException(source.type, target.type, "Copy types don't match!");
	}
	source.Normalify();

	assert(target.vector_type == VectorType::FLAT_VECTOR);
	assert(!target.sel_vector());
	assert(offset <= cardinality.count);
	target.SetCount(cardinality.count - offset);

	VectorOperations::Exec(cardinality, [&](index_t i, index_t k) {
		target.nullmask[k - offset] = source.nullmask[i];
	}, offset);

	if (!TypeIsConstantSize(source.type)) {
		switch (source.type) {
		case TypeId::VARCHAR: {
			auto source_data = (const char **)source.GetData();
			auto target_data = (const char **)target.GetData();
			VectorOperations::Exec(
			    cardinality,
			    [&](index_t i, index_t k) {
				    if (!target.nullmask[k - offset]) {
					    target_data[k - offset] = target.AddString(source_data[i]);
				    }
			    },
			    offset);
		} break;
		case TypeId::STRUCT: {
			// the main vector only has a nullmask, so set that with offset
			// recursively apply to children
			auto &source_children = source.GetChildren();
			for (auto &child : source_children) {
				auto child_copy = make_unique<Vector>(child.second->type);

				VectorOperations::Copy(*child.second, *child_copy, cardinality, offset);
				target.AddChild(move(child_copy), child.first);
			}
		} break;
		case TypeId::LIST: {
			throw NotImplementedException("FIXME: copy list");

			// // copy main vector
			// // FIXME
			// assert(offset == 0);
			// other.Initialize(TypeId::LIST);
			// other.count = count - offset;
			// other.selection_vector = nullptr;

			// // FIXME :/
			// memcpy(other.data, data, count * GetTypeIdSize(type));

			// // copy child
			// assert(children.size() == 1); // TODO if size() = 0, we would have all NULLs?
			// auto &child = children[0].second;
			// auto child_copy = make_unique<Vector>();
			// child_copy->Initialize(child->type, true, child->count);
			// child->Copy(*child_copy.get(), offset);

			// other.children.push_back(pair<string, unique_ptr<Vector>>("", move(child_copy)));
		} break;
		default:
			throw NotImplementedException("Unimplemented type for copy");
		}
	} else {
		VectorOperations::Copy(source, target.GetData(), offset, target.size());
	}
}
