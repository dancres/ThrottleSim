Two simulations based on [Monte Carlo Methods](http://www.allthingsdistributed.com/2017/02/monte-carlo-methods.html)

The first simulates a load balanced set of nodes dispatching variable duration requests in presence of a throttling 
policy. It demonstrates how imbalances in request execution time can cause additional requests to migrate to other 
machines in the network consequently breaching the throttle limit in unexpected ways.

To build and run on a Unix:

    mvn clean compile
    mvn -Dmdep.outputFile=cp.txt dependency:build-classpath
    java -classpath $(cat cp.txt):target/classes MonteCarloLB -c3 -b20 -r260000 -l40

The second simulation shows the effects of a long-tail of queries on a set of independent LRU caches (eg a cluster 
of nginx proxies). It helps in computing necessary cache sizes for a certain level of unique requests. The spread of
requests is modelled via a power curve function. It's perfectly possible to substitute this for real data or some other
distribution model.

To build and run on a Unix:

    mvn clean compile
    mvn -Dmdep.outputFile=cp.txt dependency:build-classpath
    java -classpath $(cat cp.txt):target/classes -Xmx2048m MonteCarloCache -c4 -s8 -k1000000 -h1 -z67000 -n1  

