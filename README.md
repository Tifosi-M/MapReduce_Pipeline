PMT-MapReduce
====================
[![OracleJDK](https://img.shields.io/badge/OracleJDK-v1.8.0-blue.svg)](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
[![OpenJDK](https://img.shields.io/badge/OpenJDK-v1.8.0-blue.svg)](http://openjdk.java.net/projects/jdk8/)
[![Build Status](https://travis-ci.org/szpssky/MapReduce_Pipeline.svg?branch=master)](https://travis-ci.org/szpssky/MapReduce_Pipeline)
## Overview
PMT-MapReduce(Pipelined Muthreading MapReduce) is a implement of MapReduce with Actor Model.And the PMT-MapReduce using Akka Framework.

## Actor Model
The actor model in computer science is a mathematical model of concurrent computation that treats "actors" as the universal primitives of concurrent computation. In response to a message that it receives, an actor can: make local decisions, create more actors, send more messages, and determine how to respond to the next message received. Actors may modify private state, but can only affect each other through messages (avoiding the need for any locks).
See also the [Wiki](https://en.wikipedia.org/wiki/Actor_model)

## Akka
Akka is an open-source toolkit and runtime simplifying the construction of concurrent and distributed applications on the JVM. Akka supports multiple programming models for concurrency, but it emphasizes actor-based concurrency, with inspiration drawn from Erlang.
See also the [Wiki](https://en.wikipedia.org/wiki/Akka_(toolkit))

