#include "duckdb/transaction/transaction.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"

#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/update_info.hpp"

#include <cstring>

using namespace duckdb;
using namespace std;

void Transaction::PushCatalogEntry(CatalogEntry *entry, data_ptr_t extra_data, index_t extra_data_size) {
	index_t alloc_size = sizeof(CatalogEntry *);
	if (extra_data_size > 0) {
		alloc_size += extra_data_size + sizeof(index_t);
	}
	auto baseptr = undo_buffer.CreateEntry(UndoFlags::CATALOG_ENTRY, alloc_size);
	// store the pointer to the catalog entry
	*((CatalogEntry **)baseptr) = entry;
	if (extra_data_size > 0) {
		// copy the extra data behind the catalog entry pointer (if any)
		baseptr += sizeof(CatalogEntry *);
		// first store the extra data size
		*((index_t *)baseptr) = extra_data_size;
		baseptr += sizeof(index_t);
		// then copy over the actual data
		memcpy(baseptr, extra_data, extra_data_size);
	}
}

void Transaction::PushDelete(ChunkInfo *vinfo, row_t rows[], index_t count, index_t base_row) {
	auto delete_info =
	    (DeleteInfo *)undo_buffer.CreateEntry(UndoFlags::DELETE_TUPLE, sizeof(DeleteInfo) + sizeof(row_t) * count);
	delete_info->vinfo = vinfo;
	delete_info->count = count;
	delete_info->base_row = base_row;
	memcpy(delete_info->rows, rows, sizeof(row_t) * count);
}

data_ptr_t Transaction::PushData(index_t len) {
	return undo_buffer.CreateEntry(UndoFlags::DATA, len);
}

data_ptr_t Transaction::PushString(string_t str) {
	auto entry = PushData(str.length + 1);
	memcpy(entry, str.data, str.length + 1);
	return entry;
}

UpdateInfo *Transaction::CreateUpdateInfo(index_t type_size, index_t entries) {
	auto update_info = (UpdateInfo *)undo_buffer.CreateEntry(
	    UndoFlags::UPDATE_TUPLE, sizeof(UpdateInfo) + (sizeof(sel_t) + type_size) * entries);
	update_info->max = entries;
	update_info->tuples = (sel_t *)(((data_ptr_t)update_info) + sizeof(UpdateInfo));
	update_info->tuple_data = ((data_ptr_t)update_info) + sizeof(UpdateInfo) + sizeof(sel_t) * entries;
	update_info->version_number = transaction_id;
	update_info->nullmask.reset();
	return update_info;
}

bool Transaction::Commit(WriteAheadLog *log, transaction_t commit_id) noexcept {
	this->commit_id = commit_id;

	UndoBuffer::IteratorState iterator_state;
	LocalStorage::CommitState commit_state;
	int64_t initial_wal_size;
	if (log) {
		initial_wal_size = log->GetWALSize();
	}
	bool changes_made = undo_buffer.ChangesMade() || storage.ChangesMade() || sequence_usage.size() > 0;
	try {
		// commit the undo buffer
		undo_buffer.Commit(iterator_state, log, commit_id);
		storage.Commit(commit_state, *this, log, commit_id);
		if (log) {
			// commit any sequences that were used to the WAL
			for (auto &entry : sequence_usage) {
				log->WriteSequenceValue(entry.first, entry.second);
			}
			// flush the WAL
			if (changes_made) {
				log->Flush();
			}
		}
		return true;
	} catch (Exception &ex) {
		undo_buffer.RevertCommit(iterator_state, transaction_id);
		storage.RevertCommit(commit_state);
		if (log && changes_made) {
			// remove any entries written into the WAL by truncating it
			log->Truncate(initial_wal_size);
		}
		return false;
	}
}
