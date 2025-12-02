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

// producer.cpp                                                       -*-C++-*-

// This file is part of the BlazingMQ tutorial "Re-sequencing Buffer Pattern".
//
// This producer demonstrates how to stamp messages with strictly increasing
// sequence IDs and optional entity IDs (for partitioning) to enable
// guaranteed ordering on the consumer side.  The sequence ID is added as
// a message property that consumers can use to resequence messages.
//
// Key concepts:
//   - Each message gets a monotonically increasing sequence ID
//   - Entity ID (e.g., user_id, order_id) allows per-entity sequencing
//   - Consumer can use these IDs to ensure correct processing order

// BMQ
#include <bmqa_closequeuestatus.h>
#include <bmqa_event.h>
#include <bmqa_messageeventbuilder.h>
#include <bmqa_messageproperties.h>
#include <bmqa_openqueuestatus.h>
#include <bmqa_queueid.h>
#include <bmqa_session.h>
#include <bmqt_queueflags.h>
#include <bmqt_resultcode.h>

// BDE
#include <bsl_iostream.h>
#include <bsl_ostream.h>
#include <bsl_string.h>
#include <bsl_vector.h>
#include <bsla_annotations.h>
#include <bsls_types.h>

using namespace BloombergLP;

namespace {

// CONSTANTS
const char k_QUEUE_URL[]          = "bmq://bmq.test.mem.priority/resequencing-queue";
const int  k_QUEUE_ID             = 1;
const char k_SEQ_ID_PROPERTY[]    = "seq";
const char k_ENTITY_ID_PROPERTY[] = "entity_id";

// Test messages with entity prefixes (entity:message format)
typedef bsl::vector<bsl::string> TestMessages;

}  // close unnamed namespace

//=============================================================================
//                                PRODUCER
//-----------------------------------------------------------------------------

/// Send event containing message with the specified `text`, `entityId`,
/// and `sequenceId` to the queue with the specified `queueId`,
/// using the specified `session`.
static bool postSequencedMessage(const bsl::string&   text,
                                 const bsl::string&   entityId,
                                 bsls::Types::Int64   sequenceId,
                                 const bmqa::QueueId& queueId,
                                 bmqa::Session*       session)
{
    // Build a 'MessageEvent' containing a single message with sequence ID
    bmqa::MessageEventBuilder builder;
    session->loadMessageEventBuilder(&builder);

    // Create message properties with sequence ID
    bmqa::MessageProperties properties;
    session->loadMessageProperties(&properties);

    // Add sequence ID property - crucial for ordering
    int rc = properties.setPropertyAsInt64(k_SEQ_ID_PROPERTY, sequenceId);
    if (rc != 0) {
        bsl::cerr << "Failed to set sequence ID property: " << rc << "\n";
        return false;  // RETURN
    }

    // Add entity ID if provided (for per-entity partitioning)
    if (!entityId.empty()) {
        rc = properties.setPropertyAsString(k_ENTITY_ID_PROPERTY, entityId);
        if (rc != 0) {
            bsl::cerr << "Failed to set entity ID property: " << rc << "\n";
            return false;  // RETURN
        }
    }

    // Create and configure message
    bmqa::Message& message = builder.startMessage();
    message.setPropertiesRef(&properties);
    message.setDataRef(text.c_str(), text.length());

    rc = builder.packMessage(queueId);
    if (rc != 0) {
        bsl::cerr << "Failed to pack message: rc: "
                  << bmqt::EventBuilderResult::Enum(rc) << "\n";
        return false;  // RETURN
    }

    const bmqa::MessageEvent& messageEvent = builder.messageEvent();
    rc                                     = session->post(messageEvent);
    if (rc != 0) {
        bsl::cerr << "Failed to post message: rc: " << rc << "\n";
        return false;  // RETURN
    }

    bsl::cout << "Posted: seq=" << sequenceId;
    if (!entityId.empty()) {
        bsl::cout << ", entity=" << entityId;
    }
    bsl::cout << ", data=\"" << text << "\"\n";

    return true;
}

/// Open a queue with the specified `session` and send sequenced messages.
static void produce(bmqa::Session* session)
{
    bmqa::QueueId         queueId(k_QUEUE_ID);
    bmqa::OpenQueueStatus openStatus = session->openQueueSync(
        &queueId,
        k_QUEUE_URL,
        bmqt::QueueFlags::e_WRITE);

    if (!openStatus) {
        bsl::cerr << "Failed to open queue: '" << k_QUEUE_URL
                  << "', status: " << openStatus << "\n";
        return;  // RETURN
    }

    bsl::cout << "Queue ['" << k_QUEUE_URL << "'] has been opened.\n";
    bsl::cout << "=== Re-sequencing Buffer Pattern Producer ===\n";
    bsl::cout << "Sending sequenced messages...\n\n";

    // Test messages - mix of global and per-entity messages
    // Format: {entity_id, message_text} - empty entity means global
    struct MessageData {
        const char*        entityId;
        const char*        text;
        bsls::Types::Int64 expectedSeq;
    };

    // These messages demonstrate:
    // 1. Global sequencing (empty entity)
    // 2. Per-entity sequencing (user1, user2)
    const MessageData testMessages[] = {
        {"",      "Global message 1", 1},
        {"user1", "User1 message 1",  1},
        {"user2", "User2 message 1",  1},
        {"",      "Global message 2", 2},
        {"user1", "User1 message 2",  2},
        {"",      "Global message 3", 3},
        {"user2", "User2 message 2",  2},
        {"user1", "User1 message 3",  3},
        {"",      "Global message 4", 4},
        {"user2", "User2 message 3",  3}
    };

    const int numMessages = sizeof(testMessages) / sizeof(testMessages[0]);

    for (int i = 0; i < numMessages; ++i) {
        if (!postSequencedMessage(testMessages[i].text,
                                  testMessages[i].entityId,
                                  testMessages[i].expectedSeq,
                                  queueId,
                                  session)) {
            break;
        }
    }

    bsl::cout << "\nAll messages sent.\n";

    bmqa::CloseQueueStatus closeStatus = session->closeQueueSync(&queueId);
    if (!closeStatus) {
        bsl::cerr << "Failed to close queue: '" << k_QUEUE_URL
                  << "', status: " << closeStatus << "\n";
    }
}

//=============================================================================
//                              MAIN PROGRAM
//-----------------------------------------------------------------------------

int main(BSLA_UNUSED int argc, BSLA_UNUSED const char* argv[])
{
    bsl::cout << "Starting Re-sequencing Pattern Producer...\n";

    // Start the session with the BlazingMQ broker
    bmqa::Session session;
    int           rc = session.start();
    if (rc != 0) {
        bsl::cerr << "Failed to start the session with the BlazingMQ broker"
                  << ", rc: " << bmqt::GenericResult::Enum(rc) << "\n";
        return rc;  // RETURN
    }

    produce(&session);

    bsl::cout << "Stopping session...\n";
    session.stop();

    return 0;
}
