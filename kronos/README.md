**Don't use this code.  It's hardly in the design phase yet:)**
 
# Two use-cases

## High-throughput logging system
This is a traditional multi-writer event logging system that should not get blocked on writes, and is typically write-mostly with high-throughput reads.

## Low-latency user timeline storage
You should be able to build a CRM and store user event timelines here.  The write path is similar to a logging system in that a random update to a user's timeline can happen at any time.  What is different is that the read path should have low, predictable latency, and typically won't read a lot of data.

# Requirements

## Low latency with small enough buckets
This necessitates an index so that for any stream, we can look up which buckets contain relevant information without reading through all buckets

## High throughput with large enough buckets
We should be able to saturate a network interface given enough buckets (multiple hard drive spindles) and large enough buckets (long sequential scans)

## Limit excessive scans + seeks
It should be possible to favor limited scans and seeks, depending on the application.  High-throughput scenarios should allow you to avoid having to read buckets that contain very little of your information (perhaps by limiting the time-width of any bucket?).  Low-latency scenarios should allow you to avoid having to read tens of buckets to retrieve your data.

## Consistent indexes and buckets
While cleanup processes might optimize disk layout, we should not rely on out-of-bound processes to get the content in a state where it can be read.  The read path shouldn't be dependent on the system implementation.

# Limitations

Life isn't perfect when you [use timestamps to identify event order in a distributed system](http://aphyr.com/posts/299-the-trouble-with-timestamps).

No support for push-down filters.

Users must currently pick their backends intelligently for write/read/throughput/latency tradeoffs.

# Design

## The index
A fixed time-width bucket that stores the bucket IDs of other buckets.  Should get updated in real-time by the write path.  Updating the index should be a low-latency operation.  Reading from the index might require some post-processing (e.g., sorting the information at a key), but shouldn't require a lot of random reads.

## Fixed-size, Fixed-time width buckets
Required: Streams must have byte size suggestions for buckets
Optional: A stream can specify the maximum time-width of its buckets.  This allows us to know the largest end time in a bucket a priori, which might help avoid excessive reads.  *unsure whether I want this yet*

## A background cleaner (I'd love to avoid this)
Did the time width change?  Did the bucket size change?  Did writers leave behind a bunch of small files?  Bring in the bucket brigade!
*How do avoid requiring coordination between members of the bucket brigade?*

Test, test
