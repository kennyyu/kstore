Kolumn Store
============
Kenny Yu
CS165 - Data Systems


About
=====

This is the column store I built as a project for my data systems class.
It supports a limited form of SQL, including operations for:

* creating unsorted, sorted, and btree columns
* point selections and range selections over unsorted, sorted, and btree columns
* fetching over unsorted, sorted, and btree columns
* performing joins (loop, merge, sorted, and hash joins)
* insertions, deletions, and updates on unsorted columns only

For examples, see the tests
[here](https://bitbucket.org/kennaryisland/cs165-project-tests).


Building the client and server
==============================

When cloning for the first time:

    git submodule init
    git submodule update

To build the client and server:

    make

This should create the `server` and `client` binaries in the top level
directory.

To run my own unit tests:

    make test

This will create a `testbin` directory with the test executables,
and execute them.



Running the client and server
=============================

To see available options on the client and server, run
each of them with the `--help` flag:

    ./server --help
    ./client --help

## Server options:

Here are the options on the server:

    Usage: ./server
    --help
    --port P        [default=5000]    port to start the server
    --backlog B     [default=16]      backlog for accepting connections
    --nthreads T    [default=16]      number of threads for threadpool
    --dbdir dir     [default=db]      directory for column storage
                                      if the directory already exists,
                                      the database storage will be initialized
                                      with the data from that directory.

## Client options:

Here are the options on the client:

    Usage: ./client
    --help
    --port P        [default=5000]       port of the server
    --host H        [default=127.0.0.1]  host of the server
    --loaddir dir   [default=.]          directory containing the csv files
                                         referenced in tests.
                                         For project2, this should be p2tests.
    --interactive                        Run in interactive mode



Running tests
=============

To run all the end-to-end tests [the server must already be running]:

    ./test.sh testdir

Example:

    ./test.sh p2tests
    ./test.sh p3tests
    ./test.sh p4tests

Output:

    storing output in tmp.14047/
    [PASS] p2tests/p2test-tuples.txt
    [PASS] p2tests/p2test1a.txt
    [PASS] p2tests/p2test1b.txt
    [PASS] p2tests/p2test1c.txt
    [PASS] p2tests/p2test2.txt
    [PASS] p2tests/p2test3.txt
    [PASS] p2tests/p2test4.txt
    [PASS--Same lines in different order] p2tests/p2test5.txt
    [PASS] p2tests/p2test6.txt

This will find all the `.txt` files in the provided `testdir` directory,
sort them, and then spawn clients, feeding the text file as input.
The runner will create a randomly-generated temporary directory
to store all outputs, and then diff the output against the
`.expected` files in the `testdir` directory. For outputs
that return values/tuples in a different order, the script
will first sort the expected and actual outputs before diffing.

Performance tests
=================

The performance tests attempt to perform the following query:

    SELECT MAX(r.rd), MIN(s.sg), COUNT(r.rd), COUNT(s.sg)
    FROM r,s
    WHERE r.ra = s.sa
        AND r.rc >= 1
        AND r.rc <= 9
        AND s.sf >= 31
        AND s.sf <= 99;

The perftests/gen.py script will generate workloads. To see the options:

    python -h

    usage: performance test [-h] [--seed SEED] [--rfile RFILE] [--sfile SFILE]
                            [--numr NUMR] [--nums NUMS] [--amax AMAX]
                            [--selrater SELRATER] [--selrates SELRATES]
                            outdir

    positional arguments:
      outdir               output dir

    optional arguments:
      -h, --help           show this help message and exit
      --seed SEED          random generator seed
      --rfile RFILE        output r csv file
      --sfile SFILE        output s csv file
      --numr NUMR          num rows for r
      --nums NUMS          num rows for s
      --amax AMAX          max diff a values
      --selrater SELRATER  selectivity rate r
      --selrates SELRATES  selectivity rate s

To run the performance tests, first generate a workload:

    cd perftests
    python gen.py r100000 \
           --seed=42 \
           --numr=100000 \
           --nums=10000 \
           --amax=1000 \
           --selrater=0.75 \
           --selrates=1.0

This will create a r100000 directory, where the r columns will have
100000 rows, the s columns will have 10000 rows, and the keys
to span the r.ra = s.sa join will span from 0 to amax. In the
r500000 will also be sql scripts and test scripts to run and get
the time to perform the query in mysql and in my database.

    cd r100000
    ./test_sql.sh # run and time the mysql query
    ./test_server.sh # start the server, do this in a separate shell
    ./test_client {hash | client | sort | loop} # perform the join in my db


NOTES
=====

* Since p2test1c.txt inserts (instead of loading)
  values into t1a, t2a, t2c without inserting
  corresponding values into the other columns of that table, then
  after running p2test1c.txt, we can no longer run p2test1a.txt and
  p2test1b.txt because there will be rows with extra t1a, t2a, and t2c
  values, breaking the assertion that the selections always return
  the same number of items.

* I modified p3test1.txt to perform a full selection on the right column
  before attempting to perform a treejoin. I require that the right
  input to a treejoin must be values from a full selection on the
  original column.
