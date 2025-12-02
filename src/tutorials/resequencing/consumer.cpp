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

// consumer.cpp                                                       -*-C++-*-

// This file is part of the BlazingMQ tutorial "Re-sequencing Buffer Pattern".
//
// This consumer demonstrates the Re-sequencing Buffer Pattern using LMDB
// for guaranteed message ordering.  Messages may arrive out of order from
// BlazingMQ, but this pattern ensures they are processed in sequence.
//
// The Algorithm:
// 1. On Receive: Store message in LMDB with key = QueuePrefix + SequenceID
// 2. Processor Loop: Check if the "next expected" message exists in LMDB
//    - If YES: Process it, update NEXT_EXPECTED_SEQ, delete from LMDB, ACK
//    - If NO: Wait for more messages
//
// Key concepts:
//   - Messages are buffered in LMDB until they can be processed in order
//   - Per-entity sequencing allows independent ordering for different entities
//   - LMDB provides durability - messages survive consumer restart

// BMQ
#include <bmqa_closequeuestatus.h>
#include <bmqa_confirmeventbuilder.h>
#include <bmqa_configurequeuestatus.h>
#include <bmqa_event.h>
#include <bmqa_message.h>
#include <bmqa_messageiterator.h>
#include <bmqa_messageproperties.h>
#include <bmqa_openqueuestatus.h>
#include <bmqa_queueid.h>
#include <bmqa_session.h>
#include <bmqt_messageeventtype.h>
#include <bmqt_queueflags.h>
#include <bmqt_resultcode.h>

// BDE
#include <bdlbb_blob.h>
#include <bdlbb_blobutil.h>
#include <bsl_cstring.h>
#include <bsl_functional.h>
#include <bsl_iostream.h>
#include <bsl_memory.h>
#include <bsl_sstream.h>
#include <bsl_string.h>
#include <bsl_unordered_map.h>
#include <bsl_vector.h>
#include <bsla_annotations.h>
#include <bslmt_condition.h>
#include <bslmt_lockguard.h>
#include <bslmt_mutex.h>
#include <bsls_types.h>

// LMDB
#include <lmdb.h>

// System
#include <signal.h>

using namespace BloombergLP;

namespace {

// CONSTANTS
const char   k_QUEUE_URL[]           = "bmq://bmq.test.mem.priority/resequencing-queue";
const char   k_SEQ_ID_PROPERTY[]     = "seq";
const char   k_ENTITY_ID_PROPERTY[]  = "entity_id";
const char   k_DEFAULT_ENTITY[]      = "_global_";
const char   k_NEXT_SEQ_KEY_PREFIX[] = "next_seq_";
const char   k_MESSAGE_KEY_PREFIX[]  = "msg_";
const size_t k_LMDB_MAP_SIZE         = 100 * 1024 * 1024;  // 100 MB

// GLOBAL VARIABLES
bsl::function<void(int)> g_shutdownHandler;

void signalHandler(int signal)
{
    if (g_shutdownHandler) {
        g_shutdownHandler(signal);
    }
}

// ===========================
// struct BufferedMessage
// ===========================

/// Represents a message stored in the re-sequencing buffer.
struct BufferedMessage {
    bsls::Types::Int64 d_sequenceId;
    bsl::string        d_entityId;
    bsl::string        d_payload;
};

// ===========================
// class ResequencingBuffer
// ===========================

/// LMDB-backed buffer for message resequencing.
/// Stores messages until they can be processed in sequence order.
class ResequencingBuffer {
  private:
    // DATA
    MDB_env*             d_env;
    MDB_dbi              d_messagesDbi;
    MDB_dbi              d_stateDbi;
    bsl::string          d_dbPath;
    mutable bslmt::Mutex d_mutex;
    bool                 d_isOpen;

    // PRIVATE MANIPULATORS
    bsl::string makeMessageKey(const bsl::string& entityId,
                               bsls::Types::Int64 seqId) const
    {
        bsl::ostringstream oss;
        oss << k_MESSAGE_KEY_PREFIX << entityId << "_" << seqId;
        return oss.str();
    }

    bsl::string makeNextSeqKey(const bsl::string& entityId) const
    {
        return bsl::string(k_NEXT_SEQ_KEY_PREFIX) + entityId;
    }

  public:
    // CREATORS
    explicit ResequencingBuffer(const bsl::string& dbPath)
    : d_env(0)
    , d_messagesDbi(0)
    , d_stateDbi(0)
    , d_dbPath(dbPath)
    , d_mutex()
    , d_isOpen(false)
    {
    }

    ~ResequencingBuffer()
    {
        close();
    }

    // MANIPULATORS

    /// Open the LMDB database.  Returns 0 on success.
    int open()
    {
        bslmt::LockGuard<bslmt::Mutex> guard(&d_mutex);

        if (d_isOpen) {
            return 0;
        }

        int rc = mdb_env_create(&d_env);
        if (rc != 0) {
            bsl::cerr << "Failed to create LMDB environment: "
                      << mdb_strerror(rc) << "\n";
            return rc;
        }

        rc = mdb_env_set_maxdbs(d_env, 2);
        if (rc != 0) {
            mdb_env_close(d_env);
            d_env = 0;
            return rc;
        }

        rc = mdb_env_set_mapsize(d_env, k_LMDB_MAP_SIZE);
        if (rc != 0) {
            mdb_env_close(d_env);
            d_env = 0;
            return rc;
        }

        rc = mdb_env_open(d_env, d_dbPath.c_str(), 0, 0664);
        if (rc != 0) {
            bsl::cerr << "Failed to open LMDB environment: "
                      << mdb_strerror(rc) << "\n";
            mdb_env_close(d_env);
            d_env = 0;
            return rc;
        }

        MDB_txn* txn = 0;
        rc = mdb_txn_begin(d_env, 0, 0, &txn);
        if (rc != 0) {
            mdb_env_close(d_env);
            d_env = 0;
            return rc;
        }

        rc = mdb_dbi_open(txn, "messages", MDB_CREATE, &d_messagesDbi);
        if (rc != 0) {
            mdb_txn_abort(txn);
            mdb_env_close(d_env);
            d_env = 0;
            return rc;
        }

        rc = mdb_dbi_open(txn, "state", MDB_CREATE, &d_stateDbi);
        if (rc != 0) {
            mdb_txn_abort(txn);
            mdb_env_close(d_env);
            d_env = 0;
            return rc;
        }

        rc = mdb_txn_commit(txn);
        if (rc != 0) {
            mdb_env_close(d_env);
            d_env = 0;
            return rc;
        }

        d_isOpen = true;
        return 0;
    }

    void close()
    {
        bslmt::LockGuard<bslmt::Mutex> guard(&d_mutex);
        if (d_env) {
            mdb_env_close(d_env);
            d_env = 0;
        }
        d_isOpen = false;
    }

    /// Store a message in the buffer.  Returns 0 on success.
    int storeMessage(const BufferedMessage& msg)
    {
        bslmt::LockGuard<bslmt::Mutex> guard(&d_mutex);

        if (!d_isOpen) {
            return -1;
        }

        bsl::string key = makeMessageKey(msg.d_entityId, msg.d_sequenceId);

        // Serialize: seqId|entityId|payloadLen|payload
        bsl::ostringstream oss;
        oss << msg.d_sequenceId << "|"
            << msg.d_entityId << "|"
            << msg.d_payload.length() << "|"
            << msg.d_payload;
        bsl::string serialized = oss.str();

        MDB_txn* txn = 0;
        int rc = mdb_txn_begin(d_env, 0, 0, &txn);
        if (rc != 0) {
            return rc;
        }

        MDB_val mdbKey;
        mdbKey.mv_size = key.length();
        mdbKey.mv_data = const_cast<char*>(key.c_str());

        MDB_val mdbData;
        mdbData.mv_size = serialized.length();
        mdbData.mv_data = const_cast<char*>(serialized.c_str());

        rc = mdb_put(txn, d_messagesDbi, &mdbKey, &mdbData, 0);
        if (rc != 0) {
            mdb_txn_abort(txn);
            return rc;
        }

        return mdb_txn_commit(txn);
    }

    /// Get a message from the buffer.  Returns true if found.
    bool getMessage(const bsl::string& entityId,
                    bsls::Types::Int64 seqId,
                    BufferedMessage*   msg) const
    {
        bslmt::LockGuard<bslmt::Mutex> guard(&d_mutex);

        if (!d_isOpen) {
            return false;
        }

        bsl::string key = makeMessageKey(entityId, seqId);

        MDB_txn* txn = 0;
        int rc = mdb_txn_begin(d_env, 0, MDB_RDONLY, &txn);
        if (rc != 0) {
            return false;
        }

        MDB_val mdbKey;
        mdbKey.mv_size = key.length();
        mdbKey.mv_data = const_cast<char*>(key.c_str());

        MDB_val mdbData;
        rc = mdb_get(txn, d_messagesDbi, &mdbKey, &mdbData);
        mdb_txn_abort(txn);

        if (rc != 0) {
            return false;
        }

        // Deserialize
        bsl::string serialized(static_cast<char*>(mdbData.mv_data),
                               mdbData.mv_size);

        size_t pos1 = serialized.find('|');
        size_t pos2 = serialized.find('|', pos1 + 1);
        size_t pos3 = serialized.find('|', pos2 + 1);

        if (pos1 == bsl::string::npos || pos2 == bsl::string::npos ||
            pos3 == bsl::string::npos) {
            return false;
        }

        // Safely parse numeric values with validation
        bsls::Types::Int64 seqIdParsed = 0;
        int                payloadLen  = 0;

        // Parse sequence ID safely with overflow protection
        bsl::string seqStr = serialized.substr(0, pos1);
        if (seqStr.empty()) {
            return false;  // Empty sequence ID
        }
        const bsls::Types::Int64 k_MAX_SEQ = 9223372036854775807LL / 10;
        for (size_t i = 0; i < seqStr.length(); ++i) {
            if (seqStr[i] < '0' || seqStr[i] > '9') {
                return false;  // Invalid character
            }
            if (seqIdParsed > k_MAX_SEQ) {
                return false;  // Would overflow
            }
            seqIdParsed = seqIdParsed * 10 + (seqStr[i] - '0');
        }

        // Parse payload length safely with overflow protection
        bsl::string lenStr = serialized.substr(pos2 + 1, pos3 - pos2 - 1);
        if (lenStr.empty()) {
            return false;  // Empty payload length
        }
        const int k_MAX_LEN = 2147483647 / 10;
        for (size_t i = 0; i < lenStr.length(); ++i) {
            if (lenStr[i] < '0' || lenStr[i] > '9') {
                return false;  // Invalid character
            }
            if (payloadLen > k_MAX_LEN) {
                return false;  // Would overflow
            }
            payloadLen = payloadLen * 10 + (lenStr[i] - '0');
        }

        // Validate payload length against remaining data
        size_t remainingLen = serialized.length() - (pos3 + 1);
        if (payloadLen < 0 || static_cast<size_t>(payloadLen) > remainingLen) {
            return false;  // Invalid payload length
        }

        msg->d_sequenceId = seqIdParsed;
        msg->d_entityId   = serialized.substr(pos1 + 1, pos2 - pos1 - 1);
        msg->d_payload    = serialized.substr(pos3 + 1, payloadLen);

        return true;
    }

    /// Delete a message from the buffer.  Returns 0 on success.
    int deleteMessage(const bsl::string& entityId, bsls::Types::Int64 seqId)
    {
        bslmt::LockGuard<bslmt::Mutex> guard(&d_mutex);

        if (!d_isOpen) {
            return -1;
        }

        bsl::string key = makeMessageKey(entityId, seqId);

        MDB_txn* txn = 0;
        int rc = mdb_txn_begin(d_env, 0, 0, &txn);
        if (rc != 0) {
            return rc;
        }

        MDB_val mdbKey;
        mdbKey.mv_size = key.length();
        mdbKey.mv_data = const_cast<char*>(key.c_str());

        rc = mdb_del(txn, d_messagesDbi, &mdbKey, 0);
        if (rc != 0 && rc != MDB_NOTFOUND) {
            mdb_txn_abort(txn);
            return rc;
        }

        return mdb_txn_commit(txn);
    }

    /// Get the next expected sequence ID for an entity.  Returns 1 if not set.
    bsls::Types::Int64 getNextExpectedSeq(const bsl::string& entityId) const
    {
        bslmt::LockGuard<bslmt::Mutex> guard(&d_mutex);

        if (!d_isOpen) {
            return 1;
        }

        bsl::string key = makeNextSeqKey(entityId);

        MDB_txn* txn = 0;
        int rc = mdb_txn_begin(d_env, 0, MDB_RDONLY, &txn);
        if (rc != 0) {
            return 1;
        }

        MDB_val mdbKey;
        mdbKey.mv_size = key.length();
        mdbKey.mv_data = const_cast<char*>(key.c_str());

        MDB_val mdbData;
        rc = mdb_get(txn, d_stateDbi, &mdbKey, &mdbData);
        mdb_txn_abort(txn);

        if (rc != 0) {
            return 1;
        }

        bsl::string value(static_cast<char*>(mdbData.mv_data), mdbData.mv_size);
        return bsl::stoll(value);
    }

    /// Update the next expected sequence ID for an entity.  Returns 0 on success.
    int setNextExpectedSeq(const bsl::string& entityId, bsls::Types::Int64 nextSeq)
    {
        bslmt::LockGuard<bslmt::Mutex> guard(&d_mutex);

        if (!d_isOpen) {
            return -1;
        }

        bsl::string key = makeNextSeqKey(entityId);
        bsl::ostringstream oss;
        oss << nextSeq;
        bsl::string value = oss.str();

        MDB_txn* txn = 0;
        int rc = mdb_txn_begin(d_env, 0, 0, &txn);
        if (rc != 0) {
            return rc;
        }

        MDB_val mdbKey;
        mdbKey.mv_size = key.length();
        mdbKey.mv_data = const_cast<char*>(key.c_str());

        MDB_val mdbData;
        mdbData.mv_size = value.length();
        mdbData.mv_data = const_cast<char*>(value.c_str());

        rc = mdb_put(txn, d_stateDbi, &mdbKey, &mdbData, 0);
        if (rc != 0) {
            mdb_txn_abort(txn);
            return rc;
        }

        return mdb_txn_commit(txn);
    }

    // ACCESSORS
    bool isOpen() const { return d_isOpen; }
};

// ====================
// class MessageProcessor
// ====================

/// Processes messages in sequence order from the resequencing buffer.
class MessageProcessor {
  private:
    ResequencingBuffer*                                 d_buffer_p;
    int                                                 d_numProcessed;
    bsl::unordered_map<bsl::string, bsls::Types::Int64> d_pendingCounts;

  public:
    explicit MessageProcessor(ResequencingBuffer* buffer)
    : d_buffer_p(buffer)
    , d_numProcessed(0)
    , d_pendingCounts()
    {
    }

    void notifyMessageStored(const bsl::string& entityId)
    {
        d_pendingCounts[entityId]++;
    }

    /// Process messages for a specific entity in sequence order.
    /// Returns the number of messages processed.
    int processEntity(const bsl::string& entityId)
    {
        int processed = 0;

        while (true) {
            bsls::Types::Int64 nextSeq =
                d_buffer_p->getNextExpectedSeq(entityId);

            BufferedMessage msg;
            if (!d_buffer_p->getMessage(entityId, nextSeq, &msg)) {
                break;  // Next expected message not yet available
            }

            // Process the message (business logic)
            bsl::cout << "[PROCESSED] seq=" << msg.d_sequenceId;
            if (msg.d_entityId != k_DEFAULT_ENTITY) {
                bsl::cout << ", entity=" << msg.d_entityId;
            }
            bsl::cout << ", data=\"" << msg.d_payload << "\"\n";

            // Update state
            d_buffer_p->setNextExpectedSeq(entityId, nextSeq + 1);
            d_buffer_p->deleteMessage(entityId, nextSeq);

            ++d_numProcessed;
            ++processed;

            if (d_pendingCounts[entityId] > 0) {
                d_pendingCounts[entityId]--;
            }
        }

        return processed;
    }

    /// Process all known entities.
    int processAllEntities()
    {
        int totalProcessed = 0;
        bsl::vector<bsl::string> entities;

        for (bsl::unordered_map<bsl::string, bsls::Types::Int64>::iterator
                 it = d_pendingCounts.begin();
             it != d_pendingCounts.end();
             ++it) {
            entities.push_back(it->first);
        }

        for (size_t i = 0; i < entities.size(); ++i) {
            totalProcessed += processEntity(entities[i]);
        }

        return totalProcessed;
    }

    int numProcessed() const { return d_numProcessed; }
};

// ==================
// class EventHandler
// ==================

class EventHandler : public bmqa::SessionEventHandler {
  private:
    bmqa::Session*      d_session_p;
    ResequencingBuffer* d_buffer_p;
    MessageProcessor*   d_processor_p;
    int                 d_numReceived;
    int                 d_numOutOfOrder;

  public:
    EventHandler()
    : d_session_p(0)
    , d_buffer_p(0)
    , d_processor_p(0)
    , d_numReceived(0)
    , d_numOutOfOrder(0)
    {
    }

    void setSession(bmqa::Session* session) { d_session_p = session; }
    void setBuffer(ResequencingBuffer* buffer) { d_buffer_p = buffer; }
    void setProcessor(MessageProcessor* processor) { d_processor_p = processor; }

    void onSessionEvent(const bmqa::SessionEvent& sessionEvent)
        BSLS_KEYWORD_OVERRIDE
    {
        bsl::cout << "Session event: " << sessionEvent << "\n";
    }

    void onMessageEvent(const bmqa::MessageEvent& messageEvent)
        BSLS_KEYWORD_OVERRIDE
    {
        if (messageEvent.type() != bmqt::MessageEventType::e_PUSH) {
            return;
        }

        bmqa::ConfirmEventBuilder confirmBuilder;
        d_session_p->loadConfirmEventBuilder(&confirmBuilder);

        bmqa::MessageIterator msgIter = messageEvent.messageIterator();
        while (msgIter.nextMessage()) {
            const bmqa::Message& msg = msgIter.message();
            ++d_numReceived;

            // Extract sequence ID and entity ID from properties
            bsls::Types::Int64 seqId    = 0;
            bsl::string        entityId = k_DEFAULT_ENTITY;

            if (msg.hasProperties()) {
                bmqa::MessageProperties properties;
                if (msg.loadProperties(&properties) == 0) {
                    if (properties.hasProperty(k_SEQ_ID_PROPERTY,
                                               bmqt::PropertyType::e_INT64)) {
                        seqId = properties.getPropertyAsInt64(k_SEQ_ID_PROPERTY);
                    }
                    if (properties.hasProperty(k_ENTITY_ID_PROPERTY,
                                               bmqt::PropertyType::e_STRING)) {
                        entityId =
                            properties.getPropertyAsString(k_ENTITY_ID_PROPERTY);
                    }
                }
            }

            // Get message payload
            bdlbb::Blob data;
            bsl::string payload;
            if (msg.getData(&data) == 0 && data.length() > 0) {
                payload.resize(data.length());
                bdlbb::BlobUtil::copy(&payload[0], data, 0, data.length());
            }

            bsl::cout << "[RECEIVED] seq=" << seqId;
            if (entityId != k_DEFAULT_ENTITY) {
                bsl::cout << ", entity=" << entityId;
            }
            bsl::cout << ", data=\"" << payload << "\"";

            // Check if this is the next expected message
            bsls::Types::Int64 expectedSeq =
                d_buffer_p->getNextExpectedSeq(entityId);

            if (seqId < expectedSeq) {
                bsl::cout << " [DUPLICATE]\n";
            }
            else {
                // Store in LMDB buffer
                BufferedMessage bufferedMsg;
                bufferedMsg.d_sequenceId = seqId;
                bufferedMsg.d_entityId   = entityId;
                bufferedMsg.d_payload    = payload;

                if (d_buffer_p->storeMessage(bufferedMsg) == 0) {
                    if (seqId > expectedSeq) {
                        ++d_numOutOfOrder;
                        bsl::cout << " [OUT OF ORDER - waiting for seq="
                                  << expectedSeq << "]\n";
                    }
                    else {
                        bsl::cout << " [IN ORDER]\n";
                    }
                    d_processor_p->notifyMessageStored(entityId);
                }
                else {
                    bsl::cerr << " [ERROR storing]\n";
                }
            }

            confirmBuilder.addMessageConfirmation(msg);
        }

        if (confirmBuilder.messageCount() > 0) {
            d_session_p->confirmMessages(&confirmBuilder);
        }

        // Process available messages in order
        d_processor_p->processAllEntities();
    }

    int numReceived() const { return d_numReceived; }
    int numOutOfOrder() const { return d_numOutOfOrder; }
};

}  // close unnamed namespace

//=============================================================================
//                                 CONSUMER
//-----------------------------------------------------------------------------

static void consume(bmqa::Session*    session,
                    EventHandler*     handler,
                    MessageProcessor* processor)
{
    bmqa::QueueId         queueId(bmqt::CorrelationId::autoValue());
    bmqa::OpenQueueStatus openStatus = session->openQueueSync(
        &queueId,
        k_QUEUE_URL,
        bmqt::QueueFlags::e_READ);

    if (!openStatus) {
        bsl::cerr << "Failed to open queue: " << openStatus << "\n";
        return;
    }

    bsl::cout << "\nQueue opened: " << k_QUEUE_URL << "\n";
    bsl::cout << "=== Re-sequencing Buffer Pattern Consumer ===\n";
    bsl::cout << "Messages will be processed in sequence order.\n";
    bsl::cout << "Press Ctrl+C to exit.\n\n";

    // Wait for shutdown signal
    bslmt::Mutex     mutex;
    bslmt::Condition cv;
    g_shutdownHandler = [&cv](int) {
        cv.signal();
    };
    signal(SIGINT, signalHandler);

    {
        bslmt::LockGuard<bslmt::Mutex> lock(&mutex);
        cv.wait(&mutex);
    }

    // Summary
    bsl::cout << "\n=== Summary ===\n";
    bsl::cout << "Messages received:   " << handler->numReceived() << "\n";
    bsl::cout << "Out-of-order msgs:   " << handler->numOutOfOrder() << "\n";
    bsl::cout << "Messages processed:  " << processor->numProcessed() << "\n";

    // Graceful shutdown
    bmqt::QueueOptions options;
    options.setMaxUnconfirmedMessages(0).setMaxUnconfirmedBytes(0);
    session->configureQueueSync(&queueId, options);
    session->closeQueueSync(&queueId);
}

//=============================================================================
//                              MAIN PROGRAM
//-----------------------------------------------------------------------------

int main(int argc, char* argv[])
{
    bsl::cout << "Starting Re-sequencing Pattern Consumer...\n";

    // Determine LMDB path
    bsl::string dbPath = "/tmp/resequencing_buffer";
    if (argc > 1) {
        dbPath = argv[1];
    }
    bsl::cout << "Using LMDB path: " << dbPath << "\n";

    // Create resequencing buffer
    ResequencingBuffer buffer(dbPath);
    int rc = buffer.open();
    if (rc != 0) {
        bsl::cerr << "Failed to open resequencing buffer\n";
        return rc;
    }
    bsl::cout << "Resequencing buffer opened.\n";

    // Create processor
    MessageProcessor processor(&buffer);

    // Create event handler and session
    EventHandler* eventHandler = new EventHandler();
    eventHandler->setBuffer(&buffer);
    eventHandler->setProcessor(&processor);

    bslma::ManagedPtr<bmqa::SessionEventHandler> eventHandlerMp(eventHandler);
    bmqa::Session                                session(eventHandlerMp);
    eventHandler->setSession(&session);

    rc = session.start();
    if (rc != 0) {
        bsl::cerr << "Failed to start session: "
                  << bmqt::GenericResult::Enum(rc) << "\n";
        return rc;
    }

    consume(&session, eventHandler, &processor);

    bsl::cout << "Stopping session...\n";
    session.stop();
    buffer.close();

    return 0;
}
