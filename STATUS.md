Links
=====
See https://github.com/mapr-demos/mapr-db-fast-fail-over/ for code and issue tracker.
See https://goo.gl/8ZmWAh for google doc with meeting minutes.
Hangout at https://hangouts.google.com/hangouts/_/maprtech.com/mapr-fast-failover

Mon Aug 21 08:04:56 PDT 2017
=====

Implemented synchronous behavior. This fixes timeout issue

Waiting on docker code from Ted

Master now have updated code from simplified

Fri Aug 18 08:04:56 PDT 2017
=====

Variable policies are implemented

Need timeout on open due to long open time

Need followup on docker code

Vlad and Dmitriy to do cleanup and testing

Current branch is simplified, moving master soon

Thu Aug 17 08:17:34 PDT 2017
=====

Apparently all table opens go through a single synchronized method. This means if the primary cannot be opened, it will take many seconds before the secondary can be used. This is unavoidable, apparently, so we will have to live with it, but we can avoid it impacting us after the first open of the primary table by making opens happen only on demand.
 
The proposal to make opening a table be on-demand is like this
https://gist.github.com/tdunning/5824c9f78525c1f5dd01f24765e36efe

We also talked about javadoc and the consensus was to inherit javadoc for methods, document failover in class javadoc.

Dmitriy is currently working on simplified branch, will merge to master shortly.

I provided a link to Jepsen and discussed how it does a principled job of chaos monkeying.
Jepsen link https://github.com/jepsen-io/jepsen

Ted should get info from Andy about running single node MapR in docker container to assist testing.

Actions
-------
Ted to get info on dockerizing MapR from Andy
Dmitriy and Vlad to investigate Jepsen and dockerizing MapR
Dmitriy will finish pending actions on simplified branch and merge to master (deprecate dev2 now)

Wed Aug 16 08:02:04 PDT 2017
===== 

Discussion of ted's alternative. There still are defects, but mostly these are intentional omissions. The new code doesn’t handle re-opening connections or real failover strategies. For instance, there is no stickiness (i.e. implementation will flap). The current code provides a hookpoint that will allow proper strategy integration, but doesn’t implement this yet.

There are also methods that didn't get an implementation. And there is no policy for some methods to not fail-over. This is related to the question of how to handle CheckAnd* operations. Regarding that question, we have roughly three classes of operations according to how likely the operations are to be idempotent (InsertOrReplace is relatively safe, Append is very dangerous, other operations are in the middle) and roughly three strategies (use only primary, use only current, full failover). All combinations should be configurable.

There is a problem with debugging. In the first place, it is hard to debug directly from IDEA on a dev machine because of the question about which ports to open and because there is no easy containerized debug cluster. In some cases, certain operations are taking 5 seconds or so on the test cluster, but this is really hard to debug remotely. This may have to do with setup of a connection or thread blocking in the ExecutorService. Perhaps the thread pool should be increased to 3 instead of 2 (because we inherently need at least that many connections). 

Lots of logging may help with the debugging (it is still clumsy). Casting exec to be FixedThreadPool will help because ((ThreadPoolExecutor) pool).getActiveCount() is available for logging.

Regarding the problem with Drill. The issue seems to be an install bug according to Aditya that leaves drill-memory-base.jar out of the classpath. Dmitriy to check classpaths and determine if the jar is missing or if it is a classpath problem. Ted will report an install bug when we understand the situation.

Actions
------- 
Ted to continue to get debugging help on clusters.

Vlad to investigate advanced techniques for simulating failure. (forgot to point at Jepsen for a source of techniques)

Dmitriy should continue building out failover strategies and failback and other details of the operation. There is the boring task of propagating all annotations and implementing missing methods to be done as well.

Tue Aug 15 08:03:09 PDT 2017
===== 

If no access to first db, failover instantly

Reconnection being scheduled with backoff

Question about failing CheckAnd* operations

There was a spirited discussion of the semantics of threaded executors. Ted will produce a sample implementation with provable semantics.

The sample implementation will accept a single closure

Question about pending questions from yesterday. No results, ted will force.

Actions
-------

Ted to send email to Neeraja asking for clarification about which operations should have failover. (sent)

Ted to follow up on previous questions.

Vlad to continue building QA framework and installing clusters

Dmitriy to continue from Ted's sample implementation


Mon Aug 14 08:04:50 PDT 2017
===== 

#21 Problem configuring cluster with Drill. Need help. Ted will find tech support 
on this issue.

#15 Problem on startup with down db. Thread hang. Not fully understood.
  Possibly due hanging file operation. Open? Ted will walk and talk to people about 
semantics of opening a table on an unavailable cluster.

Suggestion that user passes connections to constructor. The problem is that
QueryObject is tied to a connection. May need new issue. Definitely need better 
API clarity and may need to have overloaded Query object to help with failover.

Actions
-------

Ted to contact Neeraja and engineering to find support resources

Ted to find out more on possible cases for hard-mount style hang.

UKR team to focus on #15 primarily






