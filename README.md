# Application fast failover for MapR DB

The default MapR-DB behavior regarding data operations (read/writes) is to focus on consistency, this means sometimes during fail-over some operations have to wait for the cluster to be in a good state.

The duration of this “wait” could be long, multiple seconds, conceivably up to multiple minutes under extreme circumstances, depending of the type of failure. This “wait” could be acceptable in the context of batch oriented application, but not acceptable for any interactive/real-time services.

The goal of this demonstration is to provide a library, sample code and documentation that explains how to do fast failovers, based custom time-out, trading off the consistency in a very limited way for much higher availability.

# Project Goals

The basic idea of this project is that we want to have MapR DB, but with higher availability and much faster fail-over. Some loss of consistency is acceptable, but no user action should be required to recover consistency. As such, we call the model "asymptotic consistency".

Here are some questions and answers on the project:

> The problem that this project is solving -- basically the requirements

The goal is to have fast failover of tables with controllable loss in consistency.
 
This is required, for example, in cases such as using MapR DB in systems that touch customers.

> Is this targeted for production release / PS engagement

Currently this is targeted to be an unsupported open-source demo.

PS has expressed some interest and Neeraja may designate this for core support. Not my call.

> What is the support model (supported by PS / Your team / engineering) ?

Unsupported open source.

> Is it a general purpose solution or for specific customer needs.

General purpose (but with some limits)

> Is it a prototype or production quality 

Currently prototype, aiming at near production quality.

In particular, we have extensive unit testing and have been engaged in Aphyr/Jepsen style integration testing.
