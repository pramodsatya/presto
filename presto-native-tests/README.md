# Presto Native Tests

This module contains end-to-end tests that run queries from test classes in 
the `presto-tests` module with Presto C++ workers. Please build the module
`presto-native-execution` first (see instructions [here](https://github.com/prestodb/presto/blob/master/presto-native-execution/README.md)).

Run the following command to run all tests in this module:
```
mvn test 
    -pl 'presto-native-tests' 
    -Dtest="com.facebook.presto.nativetests.Test*" 
    -DPRESTO_SERVER=${PRESTO_HOME}/presto-native-execution/cmake-build-debug/presto_cpp/main/presto_server 
    -DWORKER_COUNT=${WORKER_COUNT} -T1C
```
Please update JVM argument `PRESTO_SERVER` to point to the Presto C++ worker
binary `presto_server`. 

## Adding new tests

Presto C++ currently does not have the same behavior as Presto for certain 
queries. This can be because of missing types, missing function signatures,
among other reasons. Tests with these unsupported queries are expected to
fail and the test ensures the error message is as expected. 

Create issues for the failing queries, so they can be documented and fixed. 
Use the tag `presto-native-tests` for these issues. Once all the failures 
in a testcase are fixed, the overriden test in this module should be removed
and the testcase in the corresponding base class in `presto-tests` would be
the single source of truth for Presto SQL coverage tests.  
