# Bounded protocol models

Run `make formal` and `python3 scripts/check_model_sensitivity.py` from the
repository root after `python3 scripts/bootstrap_formal.py`.

| Model | Contract |
|---|---|
| ShardStore | Durable acknowledgement, routing and recovery |
| ReplicaEpoch | Quorum, leader completeness, fencing and strong reads |
| TwoPhaseCommit | Membership, prepare, decision, application and release |
| PredicateLocks | Row/table compatibility, repeatable reads and phantoms |
| ReadBatch | Captured membership and read-after-ack freshness |
| VoteCommit | Immutable YES/NO votes, unanimity and completion |
| LocalFinish | Local installation/unlock after logical commitment |
| VolatileOpen | Incarnation binding, retained writes and orphan scope recovery |

The configurations are finite safety checks. Eighteen temporary mutations must
produce the named invariant violations. [Verification](../docs/verification.md)
records state counts, composition limits and trusted assumptions.
