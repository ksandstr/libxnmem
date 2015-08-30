
What (short version)
====================

This is `libxnmem`, a prototype of software transactional memory implemented in
C11. Contrary to the name, it doesn't build into a shared or static library.


(the long version)
------------------

The goal of this project is to have a mechanism for data management in
concurrent programs and multiprogrammed systems that's both more powerful than
lower-level primitives, and nicer to use than e.g. mutexes, semaphores,
rwlocks, wait/wound mutexes, seqlocks, RCU, or 2-word atomics. Software
transactional memory meets the bill by permitting composable, optimistic, and
consistent access to shared data (making it easier to design such programs than
e.g. by fine-grained locking), and by permitting simple handling of
serialization kinks not unlike what's required of SQL applications.

Currently, `libxnmem` implements the following features:

  - optimistic concurrency control
  - snapshot isolation
  - write-to-read serialization
  - safe memory reclamation

It does not have:

  - a production-quality interface [TODO]
  - deadlock detection to arbitrary depth [TODO]
  - leak-free operation [TODO]
  - a lock-free address-to-metadata mapping [TODO]
  - multi-version (MVCC) semantics [TODO]
  - two-phase commits [TODO]
  - explicit locks
  - completion guarantees in the face of pre-emption
  - write-to-read signaling (the "retry" operation)

Additionally, `libxnmem` has only been tested on an amd64 platform running both
64-bit and 32-bit test code. Its suitability for anything besides feasibility
studies, proofs-of-concept, and STM research is doubtful.


Building
--------

Have CCAN in `~/src/ccan`, `exuberant-ctags`, and perl's `prove(1)` installed.
Run `make check`. Examine the entrails.


License
-------

`libxnmem` is licensed under the GNU GPL version 3, found in the COPYING file
in this directory; or (at your option) any later version published by the Free
Software Foundation.


  -- Kalle A. Sandström <ksandstr@iki.fi>
