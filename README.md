# spark-nth-degree-connections

## Implementation


## How to Run

1. Run Tests

`test` (from sbt-shell) or `sbt "test"` (from terminal)

2. Build the project

`clean;assembly` (from sbt-shell) or `sbt "clean;assembly"` (from terminal)

2. Run the application

```bash

java -jar target/scala-2.12/calculate-nth-degree-connections.jar --input-file "/Users/prasanna/Documents/connnections.csv" --output-file "/Users/prasanna/Documents/connections/" --degrees 2
```


## Technologies/Framework used

Though this a typical graph problem and not having worked on Graphx before, I chose to do it via Spark Dataframes and SQL operations.
The reason behind spark as choice of framework is that,
   -   It is natively a distributed computing framework and can scale horizontally.
   -   Has state-of-the-art query optimizer(catalyst), predicate push-downs and physical execution engine.


## Time and Space Complexities



## Correctness of the solution

The solution scales when run in spark framework and regarding the correctness of the solution, there are unit tests in place
that verifies that all points of the requirement is met.


## Trade-offs/Limitations

The only limitation can happen I think is where I am making the tail recursion to calculate the nth degree of a connection, and they could be
 -   The recursion function will run on driver process (but still the dataframe operations should run on executors and not affect the performance.
        Also, no data is brought to the driver.)
 -   stack overflow error for the large value of N (a loop can also in place for the recursion).
Based on my experience so far, I have not used a recursion or loops in spark applications, but this is really a interesting thing to try on production 
and check the performance for the large data and the large value of N.

