# Application fast failover for MapR DB

The default MapR-DB behavior regarding data operations (read/writes) is to focus on consistency, this means sometimes during fail-over some operations have to wait for the cluster to be in a good state.

The duration of this “wait” could be long, multiple seconds, conceivably up to multiple minutes under extreme circumstances, depending of the type of failure. This “wait” could be acceptable in the context of batch oriented application, but not acceptable for any interactive/real-time services.

The goal of this demonstration is to provide a library, sample code and documentation that explains how to do fast failovers, based custom time-out, trading off the consistency in a very limited way for much higher availability.
