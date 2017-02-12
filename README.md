# ThrottleSim

Built using [Monte Carlo Methods](http://www.allthingsdistributed.com/2017/02/monte-carlo-methods.html)

To build and run on a Unix:

    mvn clean compile
    mvn -Dmdep.outputFile=cp.txt dependency:build-classpath
    java -classpath java -classpath $(cat cp.txt):target/classes MonteCarloLB -c3 -b20 -r260000 -l40
