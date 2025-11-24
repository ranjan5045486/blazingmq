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

// mqbsl_lmdblog.h                                                    -*-C++-*-
#ifndef INCLUDED_MQBSL_LMDBLOG
#define INCLUDED_MQBSL_LMDBLOG

//@PURPOSE: Provide a mechanism for reading and writing to an LMDB-backed log.
//
//@CLASSES:
//  mqbsl::LmdbLogFactory: Class used to create LMDB log instances
//  mqbsl::LmdbLog:        Mechanism for a read-write LMDB-backed log.
//
//@SEE_ALSO:
//  mqbsi::Log
//  mqbsi::LogFactory
//
//@DESCRIPTION: 'mqbsl::LmdbLog' is a mechanism for reading and writing to an
// LMDB-backed log.  LMDB (Lightning Memory-Mapped Database) provides
// persistent, ACID-compliant storage with excellent performance.  For
// efficiency, it is intended to be used in an append-only fashion.
// 'mqbsl::LmdbLogFactory' is a concrete implementation of the
// 'mqbsi::LogFactory' protocol used to create LMDB log instances.
//
/// Thread Safety
///-------------
// This component is *NOT* thread safe.

// MQB
#include <mqbsi_log.h>

#include <bmqu_blob.h>

// BDE
#include <bsl_memory.h>
#include <bsl_string.h>
#include <bslma_allocator.h>
#include <bslmf_nestedtraitdeclaration.h>
#include <bsls_keyword.h>

// LMDB
typedef struct MDB_env MDB_env;
typedef struct MDB_txn MDB_txn;
typedef unsigned int MDB_dbi;

namespace BloombergLP {

// FORWARD DECLARATION
namespace bdlbb {
class BlobBufferFactory;
}

namespace mqbsl {

// ====================
// class LmdbLogFactory
// ====================

/// Factory used to create LMDB log instances.
class LmdbLogFactory BSLS_KEYWORD_FINAL : public mqbsi::LogFactory {
  private:
    // DATA
    bslma::Allocator*         d_allocator_p;
    bdlbb::BlobBufferFactory* d_bufferFactory_p;

  private:
    // NOT IMPLEMENTED
    LmdbLogFactory(const LmdbLogFactory&) BSLS_KEYWORD_DELETED;
    LmdbLogFactory& operator=(const LmdbLogFactory&) BSLS_KEYWORD_DELETED;

  public:
    // CREATORS

    /// Constructor of a `mqbsl::LmdbLogFactory` object, using the
    /// specified `allocator` and `bufferFactory` to supply memory.
    LmdbLogFactory(bslma::Allocator*         allocator,
                   bdlbb::BlobBufferFactory* bufferFactory);

    /// Destructor.
    ~LmdbLogFactory() BSLS_KEYWORD_OVERRIDE;

    // MANIPULATORS

    /// Create a new log using the specified `config`.
    bslma::ManagedPtr<mqbsi::Log>
    create(const mqbsi::LogConfig& config) BSLS_KEYWORD_OVERRIDE;
};

// =============
// class LmdbLog
// =============

/// This class provides a mechanism for reading and writing to an
/// LMDB-backed log.
class LmdbLog BSLS_KEYWORD_FINAL : public mqbsi::Log {
  private:
    // PRIVATE TYPES
    typedef mqbsi::LogOpResult LogOpResult;
    typedef mqbsi::LogConfig   LogConfig;

    struct LogState {
        enum Enum {
            // enum representing the state of the log
            e_NON_EXISTENT     = 0,
            e_OPENED_READONLY  = 1,
            e_OPENED_READWRITE = 2,
            e_CLOSED           = 3
        };
    };

  private:
    // DATA
    bslma::Allocator* d_allocator_p;
    // Allocator to use

    bool d_isOpened;
    // Whether the log is opened.

    bsls::Types::Int64 d_totalNumBytes;
    // Total number of bytes in the log

    bsls::Types::Int64 d_outstandingNumBytes;
    // Number of outstanding bytes in the log.

    Offset d_currentOffset;
    // Current offset of the log's internal write position

    LogConfig d_config;
    // Config of the log

    LogState::Enum d_logState;
    // Current state of the log

    MDB_env* d_env;
    // LMDB environment

    MDB_dbi d_dbi;
    // LMDB database handle

    bdlbb::BlobBufferFactory* d_blobBufferFactory_p;
    // BlobBufferFactory to use, held not owned

  private:
    // NOT IMPLEMENTED
    LmdbLog(const LmdbLog&) BSLS_KEYWORD_DELETED;
    LmdbLog& operator=(const LmdbLog&) BSLS_KEYWORD_DELETED;

  private:
    // PRIVATE MANIPULATORS

    /// Write the specified `entry` into the log's internal write position.
    /// The number of outstanding bytes in the log will be incremented by
    /// the length of the `entry`.  Return the offset at which the `entry`
    /// was written on success, or a negative value on error.
    Offset writeImpl(const bdlbb::Blob& entry);

    // PRIVATE ACCESSORS

    /// Validate that the specified `length` and `offset` arguments for a
    /// `read()` or `alias()` operation are within bounds of the log.
    /// Return 0 on success or a negative value LogOpResult otherwise.
    int validateRead(int length, Offset offset) const;

  public:
    // TRAITS
    BSLMF_NESTED_TRAIT_DECLARATION(LmdbLog, bslma::UsesBslmaAllocator)

    // CREATORS

    /// Create an instance of LMDB log having the specified `config`
    /// and using the specified `blobBufferFactory`.  Memory allocations are
    /// performed using the optionally specified `allocator`.
    LmdbLog(const LogConfig&          config,
            bdlbb::BlobBufferFactory* blobBufferFactory,
            bslma::Allocator*         allocator);

    /// Destructor
    ~LmdbLog() BSLS_KEYWORD_OVERRIDE;

    // MANIPULATORS

    /// Open the log in the mode according to the specified `flags`, and
    /// return 0 on success or a negative value LogOpResult otherwise.
    int open(int flags) BSLS_KEYWORD_OVERRIDE;

    /// Close the log, and return 0 on success, or a negative value
    /// LogOpResult on error.
    int close() BSLS_KEYWORD_OVERRIDE;

    /// Move the log's internal write position to the specified `offset`,
    /// and return 0 on success, or a negative value LogOpResult on error.
    int seek(Offset offset) BSLS_KEYWORD_OVERRIDE;

    /// Increment the number of outstanding bytes in the log by the
    /// specified `value` (can be negative).
    void
    updateOutstandingNumBytes(bsls::Types::Int64 value) BSLS_KEYWORD_OVERRIDE;

    /// Update the number of outstanding bytes in the log to the specified
    /// `value`.
    void
    setOutstandingNumBytes(bsls::Types::Int64 value) BSLS_KEYWORD_OVERRIDE;

    Offset
    write(const void* entry, int offset, int length) BSLS_KEYWORD_OVERRIDE;

    /// Write the specified `length` bytes starting at the specified
    /// `offset` of the specified `entry` into the log's internal write
    /// position.
    Offset write(const bdlbb::Blob&        entry,
                 const bmqu::BlobPosition& offset,
                 int                       length) BSLS_KEYWORD_OVERRIDE;

    /// Write the specified `section` of the specified `entry` into the
    /// log's internal write position.
    Offset write(const bdlbb::Blob&       entry,
                 const bmqu::BlobSection& section) BSLS_KEYWORD_OVERRIDE;

    /// Flush any cached data up to the optionally specified `offset` to the
    /// underlying storing mechanism, and return 0 on success, or a negative
    /// value `mqbsi::LogOpResult` on error.
    int flush(Offset offset = 0) BSLS_KEYWORD_OVERRIDE;

    // ACCESSORS
    int
    read(void* entry, int length, Offset offset) const BSLS_KEYWORD_OVERRIDE;

    /// Copy the specified `length` bytes starting at the specified `offset`
    /// of the log into the specified `entry`, and return 0 on success, or a
    /// negative value LogOpResult on error.
    int read(bdlbb::Blob* entry,
             int          length,
             Offset       offset) const BSLS_KEYWORD_OVERRIDE;

    int
    alias(void** entry, int length, Offset offset) const BSLS_KEYWORD_OVERRIDE;

    /// Load into the specified `entry a reference to the specified `length'
    /// bytes starting at the specified `offset` of the log, and return 0 on
    /// success, or a negative value LogOpResult on error.
    int alias(bdlbb::Blob* entry,
              int          length,
              Offset       offset) const BSLS_KEYWORD_OVERRIDE;

    /// Return true if this log is opened, false otherwise.
    bool isOpened() const BSLS_KEYWORD_OVERRIDE;

    /// Return the total number of bytes in the log.
    bsls::Types::Int64 totalNumBytes() const BSLS_KEYWORD_OVERRIDE;

    /// Return the number of outstanding bytes in the log.
    bsls::Types::Int64 outstandingNumBytes() const BSLS_KEYWORD_OVERRIDE;

    /// Return the current offset of the log's internal write position.
    Offset currentOffset() const BSLS_KEYWORD_OVERRIDE;

    /// Return the config of the log.
    const LogConfig& logConfig() const BSLS_KEYWORD_OVERRIDE;

    /// Return true if the log supports aliasing, false otherwise.
    bool supportsAliasing() const BSLS_KEYWORD_OVERRIDE;
};

// ============================================================================
//                             INLINE DEFINITIONS
// ============================================================================

// -------------
// class LmdbLog
// -------------

// MANIPULATORS
inline void LmdbLog::updateOutstandingNumBytes(bsls::Types::Int64 value)
{
    d_outstandingNumBytes += value;
}

inline void LmdbLog::setOutstandingNumBytes(bsls::Types::Int64 value)
{
    // PRECONDITIONS
    BSLS_ASSERT_SAFE(value >= 0);

    d_outstandingNumBytes = value;
}

// ACCESSORS
inline bool LmdbLog::isOpened() const
{
    return d_isOpened;
}

inline bsls::Types::Int64 LmdbLog::totalNumBytes() const
{
    return d_totalNumBytes;
}

inline bsls::Types::Int64 LmdbLog::outstandingNumBytes() const
{
    return d_outstandingNumBytes;
}

inline mqbsi::Log::Offset LmdbLog::currentOffset() const
{
    return d_currentOffset;
}

inline const mqbsi::LogConfig& LmdbLog::logConfig() const
{
    return d_config;
}

inline bool LmdbLog::supportsAliasing() const
{
    return false;  // LMDB log does not support aliasing for safety
}

}  // close package namespace
}  // close enterprise namespace

#endif
