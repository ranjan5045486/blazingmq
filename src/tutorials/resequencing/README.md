# Re-sequencing Buffer Pattern Tutorial

This tutorial demonstrates the **Re-sequencing Buffer Pattern** - a robust solution for guaranteeing message ordering when consuming from BlazingMQ using LMDB as a persistent buffer.

## Problem Statement

In distributed systems, messages may arrive at consumers out of order due to:
- Network latency variations
- Multiple producers sending concurrently
- Message routing through different paths
- Retry mechanisms

The Re-sequencing Buffer Pattern ensures that messages are processed in the correct sequence order, even when they arrive out of order.

## The Solution

This pattern treats LMDB as a "waiting room" and "state keeper":

1. **Do not process messages immediately** when they arrive from BlazingMQ
2. **Store them in LMDB** with the sequence ID as part of the key
3. **Process only when the sequence is correct** - when the next expected message is available

## Prerequisites

### Sequence IDs
Your **Producer** must stamp every message with a strictly increasing Sequence ID (e.g., `seq: 1`, `seq: 2`, ...) inside the message properties. BlazingMQ's internal IDs are not sufficient for this logic.

### Partitioning
This logic works per "entity" (e.g., per `user_id` or `order_id`). Each entity maintains its own sequence counter.

## The Algorithm

### 1. On Receive (Async Callback)
Do not process the business logic yet.

**Action:** Write the message payload into LMDB
- **Key:** `msg_<entity_id>_<sequence_id>` (e.g., `msg_user1_105`)
- **Value:** The serialized message payload

**ACK Strategy:** Confirm the message to BlazingMQ immediately (trusting LMDB persistence), or delay ACK until processing is complete for extra safety.

### 2. The Processor Loop
This loop checks if the "next needed message" has arrived.

**State:** LMDB stores a variable `NEXT_EXPECTED_SEQ` per entity. Let's say it is `1`.

**Check:** Does LMDB contain the key `msg_user1_1`?

- **If YES:**
  1. Read payload from `msg_user1_1`
  2. Process it (Business Logic)
  3. Update `NEXT_EXPECTED_SEQ` to `2` in LMDB
  4. Delete `msg_user1_1` from LMDB
  5. ACK the message to BlazingMQ (if not already done)
  6. Loop Immediately: Check if `msg_user1_2` is waiting

- **If NO:**
  - Stop. Wait for more data.
  - Note: Message 2 might be in LMDB, but we ignore it until message 1 arrives.

## Files in This Tutorial

- `producer.cpp` - Demonstrates how to stamp messages with sequence IDs
- `consumer.cpp` - Implements the re-sequencing buffer pattern with LMDB

## Building

```bash
# From the BlazingMQ build directory
cmake --build . --target resequencing
```

## Running

### Start the Consumer First
```bash
./consumer.tsk [lmdb_path]
# Default LMDB path: /tmp/resequencing_buffer
```

### Then Run the Producer
```bash
./producer.tsk
```

## Example Output

### Producer
```
Starting Re-sequencing Pattern Producer...
Queue ['bmq://bmq.test.mem.priority/resequencing-queue'] has been opened.
=== Re-sequencing Buffer Pattern Producer ===
Sending sequenced messages...

Posted: seq=1, data="Global message 1"
Posted: seq=1, entity=user1, data="User1 message 1"
Posted: seq=1, entity=user2, data="User2 message 1"
Posted: seq=2, data="Global message 2"
...
```

### Consumer
```
Starting Re-sequencing Pattern Consumer...
Using LMDB path: /tmp/resequencing_buffer
Resequencing buffer opened.

Queue opened: bmq://bmq.test.mem.priority/resequencing-queue
=== Re-sequencing Buffer Pattern Consumer ===
Messages will be processed in sequence order.
Press Ctrl+C to exit.

[RECEIVED] seq=1, data="Global message 1" [IN ORDER]
[PROCESSED] seq=1, data="Global message 1"
[RECEIVED] seq=1, entity=user1, data="User1 message 1" [IN ORDER]
[PROCESSED] seq=1, entity=user1, data="User1 message 1"
...
```

## Key Features

1. **Per-entity Sequencing**: Each entity (user, order, etc.) maintains its own sequence
2. **LMDB Persistence**: Messages survive consumer restart
3. **Out-of-order Handling**: Later messages are buffered until earlier ones arrive
4. **Duplicate Detection**: Already-processed messages are ignored

## Production Considerations

1. **ACK Strategy**: Consider delaying ACK until the message is actually processed for at-least-once delivery
2. **LMDB Size**: Monitor and configure appropriate map size for your workload
3. **Cleanup**: Implement cleanup for orphaned messages (gaps that will never be filled)
4. **Monitoring**: Add metrics for buffer size, out-of-order count, processing lag

## Related Documentation

- [BlazingMQ Message Properties](https://bloomberg.github.io/blazingmq/)
- [LMDB Documentation](http://www.lmdb.tech/doc/)
