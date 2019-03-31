Change Log
==========

v1.4.1 (Mar. 31, 2019)
----------------------

- fix pending tx not found issue.
  the pending tx-id cannot find its corresponding transaction from node (node is not fully syncronized with peers),
  it is ignored and will be processed later.


v1.4.0 (Mar. 30, 2019)
----------------------

- adds config::resolve_spending;
  resolve spending details (address, value, height) can be very time consuming when the total number of pending transactions is huge,
  the spending details are only needed by api getBalance()::spending optional fields.
- fix issue that the pending records are not purged between different sessions.

v1.3.2 (Mar. 25, 2019)
----------------------

- refactorize the sampling loop to avoid possible memory leakage;
- adds memory leakage detection logic; (DEBUG_MEM_LEAK=0*,1,2)
  - 0: no detection;
  - 1: leak+stats;
  - 2: leak+stats+heap_diff;


v1.3.1 (Dec. 31, 2018)
----------------------

- uses "mydbg" as debug module;