# gae-signals

Signals library for Google App Engine.

## What is this?

_Signals_ are used in complex applications to decouple event dispatch from event handling.
A typical use case is having one module send a signal (corresponding to some
application-specific event) and have some other module receive it at some later time,
and handle appropriately.

In web applications signals are especially useful. They allow foreground requests
to generate events which defer expensive computation for later time.
Background processes can then have such signals delivered and perform those time-consuming
tasks without increasing visible latency.

_gae-signals_ library provides simple, memcache-based implementation of signals for Google App Engine.

## Usage

