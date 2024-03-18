## Note1

The InitMigration should go through the raft and send the shardkv.Migrate RPC from there, so that all the servers in
given group can agree the timing(same order/index) at which to send the Migrate RPC with same shard state.

## Note2

There should NOT be any time-consuming operation in the Raft's Apply method, as it will block the Raft's state machine(
e.g., cause the heartbeat timeout then trigger unnecessary election etc.), since this implementation is relied on reply
the raft log to reconstruct the state machine, we can simply do some fast bookkeeping and leave the heavily work to the
outside of the Apply method and if there is any crash during the heavily work the raft log reply will bring us to the
right state as long as the effects of Apply method is idempotent.

During the bookkeeping, if there are interaction with external system(e.g., RPC call), we should make sure the
correctness when snapshot involved. e.g., when reconstruct from the snapshot the interactions in the command apply will
not be replied at all.

## Note3

A deadlock case:

1. Put shardkv.Migrate RPC call in opInitMigrate, in the loop of receiving a msg from applyCh
2. And the shardkv.Migrate RPC block quite long time(e.g., 3 seconds) and in the meanwhile
3. The underlying raft is holding the lock while waiting send the entry to applyCh
4. The raft heartbeat goroutine is blocking on acquiring the lock and cause the follower thought the leader(s1) is
   dead then the follower elected a new leader(s2)
5. The shardkv.Migrate RPC at step #2 is returns, the old leader(s1) learned the new leader(s2) via heartbeat and trying
   to switch to follower state by sending msg to raft.followerCh(cap is 3), meanwhile the s1 received AEs from s2
   and trying to switch to follower state by sending msg to raft.followerCh as well, the followerCh is full, but by the
   time the receiver of raft.followerCh(the ticker goroutine) is not getting chance to run and the last sender is
   blocking on sending the msg to the channel while holding the raft lock.
6. There is a snapshot request send to the snapshotCh and the snapshotCh receiver case got selected in the ticker
   goroutine, then try to acquire raft lock and block on it, the followerCh case have no chane to run, and the deadlock
   happens.

## Note4

The ApplySnapshot should apply the snapshot only if the current applied index is less than the snapshot's index, i.e.,
the snapshot is newer that the state. Otherwise, we might lose some state because of applying an older snapshot(maybe
triggered by the raft leader via the InstallSnapshot RPC call).

## Note5

The way we detect duplication is assuming that a client will make only one call into a Clerk at a time,
i.e., `requestId <= lastAppliedReqId` is enough to detect the duplicate request in case of the assumption. But if the
client can make multiple calls into a Clerk at a time, the above way will not work, for example: If there are two
request with id #1 and #2 sending to the server at the same time, maybe the request #2 arrive the server and get applied
first. by the time #1 arrive the server, it will be considered as a duplication and get ignored incorrectly.
