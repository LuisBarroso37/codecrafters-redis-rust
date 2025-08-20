## Warnings

- Setting TCP_NODELAY no the sockets seems to do nothing. Commands are still grouped together if sent in quick succession.

## Things to check or still to be implemented

- Should we be able to call WAIT commands during a transaction?
- Should the replica be able to respond to read commands from the master server?
- Should master server be able to handle PSYNC commands?
- Can we run PSYNC and REPLCONF within a transaction?
- Which commands besides the write commands should update the replication offset?
- After sending the FULLRESYNC response to the replica, how exactly does Redis do the streaming of the RDB file? At the moment in the code, the RDB file is sent in the same response as the FULLRESYNC
- Do we need 2 separate offset counters for the replicas? One for all processed commands coming from the master server and one for only the write commands? Otherwise the offset counting does not really work as it should

## Run specific integration test with info logs

```
cargo test --test redis -- commands::xread::test_handle_xread_blocking_command_direct_response --nocapture
```
