# gae-signals

Signals library for Google App Engine. **Experimental**.

## Issues

* Delivering signals is slightly paranoid about consistency
  which makes it perform one memcache request per signal
  (plus additional one for locking). This might not be necessary
  and can be potentially replaced with get_multi/set_multi, sacrificing
  some of the consistency. (Alternatively, user could choose which signals
  shall be reliably delivered and which ones not)