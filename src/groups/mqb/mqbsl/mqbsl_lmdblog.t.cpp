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

// mqbsl_lmdblog.t.cpp                                                -*-C++-*-
#include <mqbsl_lmdblog.h>

// MQB
#include <mqbsi_log.h>
#include <mqbu_storagekey.h>

// BDE
#include <bdlbb_pooledblobbufferfactory.h>
#include <bdls_filesystemutil.h>
#include <bdls_tempdirectoryguard.h>
#include <bsl_iostream.h>
#include <bslma_testallocator.h>
#include <bsls_types.h>

// TEST DRIVER
#include <mwctst_testhelper.h>

// CONVENIENCE
using namespace BloombergLP;
using namespace bsl;

// ============================================================================
//                            TEST HELPERS UTILITY
// ----------------------------------------------------------------------------
namespace {

}  // close unnamed namespace

// ============================================================================
//                                    TESTS
// ----------------------------------------------------------------------------

static void test1_breathingTest()
// ------------------------------------------------------------------------
// BREATHING TEST
//
// Concerns:
//   Exercise basic functionality before beginning testing in earnest.
//   Probe that functionality to discover basic errors.
//
// Testing:
//   Basic functionality
// ------------------------------------------------------------------------
{
    mwctst::TestHelper::printTestName("BREATHING TEST");

    bslma::TestAllocator         alloc;
    bdlbb::PooledBlobBufferFactory bufferFactory(1024, &alloc);

    // Create a temporary directory for test
    bdls::TempDirectoryGuard tempDirGuard("mqbsl_lmdblog_");
    bsl::string              dbPath(&alloc);
    dbPath = tempDirGuard.getTempDirName();
    dbPath += "/test.lmdb";

    mqbu::StorageKey logId(mqbu::StorageKey::BinaryRepresentation(), "12345");

    mqbsi::LogConfig config(1024 * 1024,  // 1 MB max size
                            logId,
                            dbPath,
                            false,  // reserveOnDisk
                            false,  // prefaultPages
                            &alloc);

    // Create factory
    mqbsl::LmdbLogFactory factory(&alloc, &bufferFactory);

    // Create log
    bslma::ManagedPtr<mqbsi::Log> log = factory.create(config);
    ASSERT(log);

    // Open log with CREATE_IF_MISSING
    int rc = log->open(mqbsi::Log::e_CREATE_IF_MISSING);
    ASSERT_EQ(rc, 0);
    ASSERT(log->isOpened());

    // Write some data
    const char* testData = "Hello LMDB!";
    const int   dataLen  = bsl::strlen(testData);

    mqbsi::Log::Offset offset1 = log->write(testData, 0, dataLen);
    ASSERT(offset1 >= 0);

    // Read it back
    char readBuffer[256];
    rc = log->read(readBuffer, dataLen, offset1);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(bsl::memcmp(readBuffer, testData, dataLen), 0);

    // Flush
    rc = log->flush();
    ASSERT_EQ(rc, 0);

    // Close
    rc = log->close();
    ASSERT_EQ(rc, 0);
    ASSERT(!log->isOpened());

    PV("LMDB log basic operations work correctly");
}

static void test2_reopenTest()
// ------------------------------------------------------------------------
// REOPEN TEST
//
// Concerns:
//   Verify that data persists across close/reopen cycles.
//
// Testing:
//   Persistence
// ------------------------------------------------------------------------
{
    mwctst::TestHelper::printTestName("REOPEN TEST");

    bslma::TestAllocator         alloc;
    bdlbb::PooledBlobBufferFactory bufferFactory(1024, &alloc);

    // Create a temporary directory for test
    bdls::TempDirectoryGuard tempDirGuard("mqbsl_lmdblog_");
    bsl::string              dbPath(&alloc);
    dbPath = tempDirGuard.getTempDirName();
    dbPath += "/test_reopen.lmdb";

    mqbu::StorageKey logId(mqbu::StorageKey::BinaryRepresentation(), "12345");

    mqbsi::LogConfig config(1024 * 1024,  // 1 MB max size
                            logId,
                            dbPath,
                            false,  // reserveOnDisk
                            false,  // prefaultPages
                            &alloc);

    mqbsl::LmdbLogFactory factory(&alloc, &bufferFactory);

    const char*        testData = "Persistent data!";
    const int          dataLen  = bsl::strlen(testData);
    mqbsi::Log::Offset offset1;

    // First session: write and close
    {
        bslma::ManagedPtr<mqbsi::Log> log = factory.create(config);
        int rc = log->open(mqbsi::Log::e_CREATE_IF_MISSING);
        ASSERT_EQ(rc, 0);

        offset1 = log->write(testData, 0, dataLen);
        ASSERT(offset1 >= 0);

        rc = log->close();
        ASSERT_EQ(rc, 0);
    }

    // Second session: reopen and read
    {
        bslma::ManagedPtr<mqbsi::Log> log = factory.create(config);
        int rc = log->open(0);  // Just open, don't create
        ASSERT_EQ(rc, 0);

        char readBuffer[256];
        rc = log->read(readBuffer, dataLen, offset1);
        ASSERT_EQ(rc, 0);
        ASSERT_EQ(bsl::memcmp(readBuffer, testData, dataLen), 0);

        rc = log->close();
        ASSERT_EQ(rc, 0);
    }

    PV("LMDB log persists data correctly");
}

// ============================================================================
//                                 MAIN PROGRAM
// ----------------------------------------------------------------------------

int main(int argc, char* argv[])
{
    TEST_PROLOG(mwctst::TestHelper::e_DEFAULT);

    switch (_testCase) {
    case 0:
    case 2: test2_reopenTest(); break;
    case 1: test1_breathingTest(); break;
    default: {
        cerr << "WARNING: CASE '" << _testCase << "' NOT FOUND." << endl;
        s_testStatus = -1;
    } break;
    }

    TEST_EPILOG(mwctst::TestHelper::e_CHECK_DEF_GBL_ALLOC);
}
