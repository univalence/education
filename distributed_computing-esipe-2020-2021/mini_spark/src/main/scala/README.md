# Mini-Spark

In `io.univalence.mini_spark`, you will find:

* `MiniSparkApplicationMain`:
  Example of application using Mini-Spark. You will need to run 2
  executors once you have launched this application.
* `ExecutorMain`:
  Main application for Executors. You need to provide an executor ID
  (which should be an integer) as a parameter when launching the
  component.
* `MockDriverMain`:
  Application to test the fault-tolerance mechanism between a driver
  and a set of executors.

In term of packages:

* `test_actor`:
  different tests of patterns with actors.
* `rdd`:
  the source code to manage RDD and partitions.
* `computation`:
  the source code to manage the distributed computation.
