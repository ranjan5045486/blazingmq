// Copyright 2024 Bloomberg Finance L.P.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// mqbsl_lmdblog.cpp                                                  -*-C++-*-
#include <mqbsl_lmdblog.h>

#include <mqbscm_version.h>

// BDE
#include <bdlbb_blobutil.h>
#include <bdls_filesystemutil.h>
#include <bdls_pathutil.h>
#include <bsla_annotations.h>
#include <bslma_allocator.h>
#include <bsls_assert.h>

// LMDB
#include <lmdb.h>

// System
#include <bsl_cstring.h>
#include <bsl_vector.h>

namespace BloombergLP {
namespace mqbsl {

namespace {

// CONSTANTS
const size_t k_LMDB_DEFAULT_MAP_SIZE = 10UL * 1024 * 1024 * 1024;  // 10GB default

/// Convert LMDB error code to LogOpResult
mqbsi::LogOpResult::Enum convertLmdbError(int rc)
{
    if (rc == 0) {
        return mqbsi::LogOpResult::e_SUCCESS;
    }
    // Map common LMDB errors to log errors
    switch (rc) {
    case MDB_NOTFOUND:
        return mqbsi::LogOpResult::e_OFFSET_OUT_OF_RANGE;
    case ENOENT:
        return mqbsi::LogOpResult::e_FILE_NOT_EXIST;
    case EACCES:
    case EAGAIN:
        return mqbsi::LogOpResult::e_FILE_OPEN_FAILURE;
    default:
        return mqbsi::LogOpResult::e_UNKNOWN;
    }
}

}  // close unnamed namespace

// --------------------
// class LmdbLogFactory
// --------------------

// CREATORS
LmdbLogFactory::LmdbLogFactory(bslma::Allocator*         allocator,
                               bdlbb::BlobBufferFactory* bufferFactory)
: d_allocator_p(allocator)
, d_bufferFactory_p(bufferFactory)
{
    // NOTHING
}

LmdbLogFactory::~LmdbLogFactory()
{
    // NOTHING
}

// MANIPULATORS
bslma::ManagedPtr<mqbsi::Log>
LmdbLogFactory::create(const mqbsi::LogConfig& config)
{
    bslma::ManagedPtr<mqbsi::Log> log(
        new (*d_allocator_p) LmdbLog(config, d_bufferFactory_p, d_allocator_p),
        d_allocator_p);
    return log;
}

// -------------
// class LmdbLog
// -------------

mqbsi::Log::Offset LmdbLog::writeImpl(const bdlbb::Blob& entry)
{
    if (d_logState != LogState::e_OPENED_READWRITE) {
        return LogOpResult::e_UNSUPPORTED_OPERATION;  // RETURN
    }

    const int length = entry.length();
    if (d_totalNumBytes + length > d_config.maxSize()) {
        return LogOpResult::e_REACHED_END_OF_LOG;  // RETURN
    }

    // Flatten blob to contiguous memory for LMDB storage
    bsl::vector<char> buffer(d_allocator_p);
    buffer.resize(length);
    bdlbb::BlobUtil::copy(buffer.data(), entry, 0, length);

    MDB_txn* txn = 0;
    int      rc  = mdb_txn_begin(d_env, 0, 0, &txn);
    if (rc != 0) {
        return convertLmdbError(rc);  // RETURN
    }

    // Store record at current offset
    MDB_val key;
    key.mv_size = sizeof(d_currentOffset);
    key.mv_data = &d_currentOffset;

    MDB_val data;
    data.mv_size = length;
    data.mv_data = buffer.data();

    rc = mdb_put(txn, d_dbi, &key, &data, 0);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return convertLmdbError(rc);  // RETURN
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        return convertLmdbError(rc);  // RETURN
    }

    Offset writeOffset = d_currentOffset;
    ++d_currentOffset;
    d_outstandingNumBytes += length;
    d_totalNumBytes += length;

    return writeOffset;
}

int LmdbLog::validateRead(int length, Offset offset) const
{
    // PRECONDITIONS
    BSLS_ASSERT_SAFE(offset >= 0);
    BSLS_ASSERT_SAFE(length >= 0);

    if (d_logState != LogState::e_OPENED_READWRITE &&
        d_logState != LogState::e_OPENED_READONLY) {
        return LogOpResult::e_UNSUPPORTED_OPERATION;  // RETURN
    }

    if (offset >= d_currentOffset) {
        return LogOpResult::e_OFFSET_OUT_OF_RANGE;  // RETURN
    }

    return LogOpResult::e_SUCCESS;
}

LmdbLog::LmdbLog(const LogConfig&          config,
                 bdlbb::BlobBufferFactory* blobBufferFactory,
                 bslma::Allocator*         allocator)
: d_allocator_p(allocator)
, d_isOpened(false)
, d_totalNumBytes(0)
, d_outstandingNumBytes(0)
, d_currentOffset(0)
, d_config(config)
, d_logState(LogState::e_NON_EXISTENT)
, d_env(0)
, d_dbi(0)
, d_blobBufferFactory_p(blobBufferFactory)
{
    // NOTHING
}

LmdbLog::~LmdbLog()
{
    if (d_isOpened) {
        close();
    }
}

int LmdbLog::open(int flags)
{
    if (d_logState == LogState::e_OPENED_READONLY ||
        d_logState == LogState::e_OPENED_READWRITE) {
        return LogOpResult::e_LOG_ALREADY_OPENED;  // RETURN
    }

    // Extract directory from location
    bsl::string dbPath(d_config.location(), d_allocator_p);
    bsl::string dbDir(d_allocator_p);

    // Get directory from full path
    int rc = bdls::PathUtil::getDirname(&dbDir, dbPath);
    if (rc != 0) {
        return LogOpResult::e_FILE_OPEN_FAILURE;  // RETURN
    }

    // Create directory if it doesn't exist and CREATE_IF_MISSING is set
    if (flags & e_CREATE_IF_MISSING) {
        if (!bdls::FilesystemUtil::exists(dbDir)) {
            rc = bdls::FilesystemUtil::createDirectories(dbDir, true);
            if (rc != 0) {
                return LogOpResult::e_FILE_OPEN_FAILURE;  // RETURN
            }
        }
    }

    // Create LMDB environment
    rc = mdb_env_create(&d_env);
    if (rc != 0) {
        return convertLmdbError(rc);  // RETURN
    }

    // Set map size - use configured max size or default
    size_t mapSize = d_config.maxSize() > 0 
                     ? static_cast<size_t>(d_config.maxSize())
                     : k_LMDB_DEFAULT_MAP_SIZE;
    rc = mdb_env_set_mapsize(d_env, mapSize);
    if (rc != 0) {
        mdb_env_close(d_env);
        d_env = 0;
        return convertLmdbError(rc);  // RETURN
    }

    // Open environment
    unsigned int envFlags = 0;
    if (flags & e_READ_ONLY) {
        envFlags |= MDB_RDONLY;
    }

    rc = mdb_env_open(d_env, dbPath.c_str(), envFlags, 0664);
    if (rc != 0) {
        if (rc == ENOENT && !(flags & e_CREATE_IF_MISSING)) {
            mdb_env_close(d_env);
            d_env = 0;
            return LogOpResult::e_FILE_NOT_EXIST;  // RETURN
        }
        mdb_env_close(d_env);
        d_env = 0;
        return convertLmdbError(rc);  // RETURN
    }

    // Open database
    MDB_txn* txn = 0;
    rc = mdb_txn_begin(d_env, 0, (flags & e_READ_ONLY) ? MDB_RDONLY : 0, &txn);
    if (rc != 0) {
        mdb_env_close(d_env);
        d_env = 0;
        return convertLmdbError(rc);  // RETURN
    }

    rc = mdb_dbi_open(txn,
                      0,  // Use unnamed database
                      (flags & e_CREATE_IF_MISSING) ? MDB_CREATE : 0,
                      &d_dbi);
    if (rc != 0) {
        mdb_txn_abort(txn);
        mdb_env_close(d_env);
        d_env = 0;
        return convertLmdbError(rc);  // RETURN
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        // Note: Database handle d_dbi is automatically closed when the 
        // environment is closed, so we don't need to explicitly close it here
        mdb_env_close(d_env);
        d_env = 0;
        d_dbi = 0;
        return convertLmdbError(rc);  // RETURN
    }

    // Scan existing records to determine current offset and sizes
    d_currentOffset       = 0;
    d_totalNumBytes       = 0;
    d_outstandingNumBytes = 0;

    MDB_txn* scanTxn = 0;
    rc = mdb_txn_begin(d_env, 0, MDB_RDONLY, &scanTxn);
    if (rc == 0) {
        MDB_cursor* cursor = 0;
        rc                 = mdb_cursor_open(scanTxn, d_dbi, &cursor);
        if (rc == 0) {
            MDB_val key, data;
            while (mdb_cursor_get(cursor, &key, &data, MDB_NEXT) == 0) {
                if (key.mv_size == sizeof(Offset)) {
                    Offset offset = *static_cast<Offset*>(key.mv_data);
                    if (offset >= d_currentOffset) {
                        d_currentOffset = offset + 1;
                    }
                    d_totalNumBytes += data.mv_size;
                    d_outstandingNumBytes += data.mv_size;
                }
            }
            mdb_cursor_close(cursor);
        }
        mdb_txn_abort(scanTxn);
    }

    d_logState = (flags & e_READ_ONLY) ? LogState::e_OPENED_READONLY
                                       : LogState::e_OPENED_READWRITE;
    d_isOpened = true;

    return LogOpResult::e_SUCCESS;
}

int LmdbLog::close()
{
    if (d_logState == LogState::e_CLOSED ||
        d_logState == LogState::e_NON_EXISTENT) {
        return LogOpResult::e_LOG_ALREADY_CLOSED;  // RETURN
    }

    if (d_env) {
        // Close database handle (implicitly done when closing env)
        mdb_env_close(d_env);
        d_env = 0;
        d_dbi = 0;
    }

    d_logState = LogState::e_CLOSED;
    d_isOpened = false;

    return LogOpResult::e_SUCCESS;
}

int LmdbLog::seek(Offset offset)
{
    if (d_logState != LogState::e_OPENED_READWRITE) {
        return LogOpResult::e_UNSUPPORTED_OPERATION;  // RETURN
    }

    if (offset < 0) {
        return LogOpResult::e_OFFSET_OUT_OF_RANGE;  // RETURN
    }

    d_currentOffset = offset;
    return LogOpResult::e_SUCCESS;
}

mqbsi::Log::Offset LmdbLog::write(const void* entry, int offset, int length)
{
    // Create a blob from the raw data
    bdlbb::Blob blob(d_blobBufferFactory_p, d_allocator_p);
    bdlbb::BlobUtil::append(
        &blob,
        static_cast<const char*>(entry) + offset,
        length);

    return writeImpl(blob);
}

mqbsi::Log::Offset LmdbLog::write(const bdlbb::Blob&        entry,
                                  const bmqu::BlobPosition& offset,
                                  int                       length)
{
    bdlbb::Blob blob(d_blobBufferFactory_p, d_allocator_p);
    bdlbb::BlobUtil::append(&blob, entry, offset, length);

    return writeImpl(blob);
}

mqbsi::Log::Offset LmdbLog::write(const bdlbb::Blob&       entry,
                                  const bmqu::BlobSection& section)
{
    bdlbb::Blob blob(d_blobBufferFactory_p, d_allocator_p);
    bdlbb::BlobUtil::append(&blob, entry, section);

    return writeImpl(blob);
}

int LmdbLog::flush(Offset offset)
{
    // LMDB transactions are already durable, but we can sync for extra safety
    if (!d_isOpened) {
        return LogOpResult::e_UNSUPPORTED_OPERATION;  // RETURN
    }

    int rc = mdb_env_sync(d_env, 1);  // Force sync
    return convertLmdbError(rc);
}

int LmdbLog::read(void* entry, int length, Offset offset) const
{
    int rc = validateRead(length, offset);
    if (rc != 0) {
        return rc;  // RETURN
    }

    MDB_txn* txn = 0;
    rc           = mdb_txn_begin(d_env, 0, MDB_RDONLY, &txn);
    if (rc != 0) {
        return convertLmdbError(rc);  // RETURN
    }

    MDB_val key;
    key.mv_size = sizeof(offset);
    key.mv_data = const_cast<Offset*>(&offset);

    MDB_val data;
    rc = mdb_get(txn, d_dbi, &key, &data);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return convertLmdbError(rc);  // RETURN
    }

    if (static_cast<int>(data.mv_size) < length) {
        mdb_txn_abort(txn);
        return LogOpResult::e_REACHED_END_OF_RECORD;  // RETURN
    }

    bsl::memcpy(entry, data.mv_data, length);
    mdb_txn_abort(txn);

    return LogOpResult::e_SUCCESS;
}

int LmdbLog::read(bdlbb::Blob* entry, int length, Offset offset) const
{
    int rc = validateRead(length, offset);
    if (rc != 0) {
        return rc;  // RETURN
    }

    MDB_txn* txn = 0;
    rc           = mdb_txn_begin(d_env, 0, MDB_RDONLY, &txn);
    if (rc != 0) {
        return convertLmdbError(rc);  // RETURN
    }

    MDB_val key;
    key.mv_size = sizeof(offset);
    key.mv_data = const_cast<Offset*>(&offset);

    MDB_val data;
    rc = mdb_get(txn, d_dbi, &key, &data);
    if (rc != 0) {
        mdb_txn_abort(txn);
        return convertLmdbError(rc);  // RETURN
    }

    if (static_cast<int>(data.mv_size) < length) {
        mdb_txn_abort(txn);
        return LogOpResult::e_REACHED_END_OF_RECORD;  // RETURN
    }

    bdlbb::BlobUtil::append(entry,
                            static_cast<const char*>(data.mv_data),
                            length);
    mdb_txn_abort(txn);

    return LogOpResult::e_SUCCESS;
}

int LmdbLog::alias(void** entry, int length, Offset offset) const
{
    // LMDB log does not support aliasing for safety reasons
    // Data must be copied out
    return LogOpResult::e_UNSUPPORTED_OPERATION;
}

int LmdbLog::alias(bdlbb::Blob* entry, int length, Offset offset) const
{
    // LMDB log does not support aliasing for safety reasons
    // Data must be copied out
    return LogOpResult::e_UNSUPPORTED_OPERATION;
}

}  // close package namespace
}  // close enterprise namespace
