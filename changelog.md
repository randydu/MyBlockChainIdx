Change Log
==========

v1.3.2 (Jan. 22, 2019)
----------------------

- refactorize the sampling loop to avoid possible memory leakage;
- adds memory leakage detection logic; (DEBUG_MEM_LEAK=0*,1,2)
  * 0: no detection;
  * 1: leak+stats;
  * 2: leak+stats+heap_diff;



v1.3.1 (Dec. 31, 2018)
----------------------

- uses "mydbg" as debug module;