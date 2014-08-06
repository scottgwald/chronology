# The Chronology Project

## Introduction

This repository contains three loosely coupled projects:

* [Kronos](kronos/) is a time series storage engine that
  allows you to store and retrieve timestamped JSON blobs from various
  backends (e.g., Cassandra, S3).  It's a way friendlier logging API
  than your filesystem.

* [Metis](metis/) is an HTTP compute service. It's currently implemented as a
  thin wrapper around the
  [Spark data processing engine](http://spark.apache.org/).

* [Jia](jia/) is a visualization, dashboarding, and data
  exploration tool.  It can speak with Kronos and Metis.  It answers
  questions for humans, rather than for developers.

## Get running in 5 minutes

Each of the links above has a "Get running in 5 minutes" section.  If
you can't get started with one of these systems in five minutes,
contact us and we'll make sure you can!

## Contributing

You're contributing to this project by using it, so thanks!  

If you run into any issues with code or documentation, please [file an
issue](../../issues?state=open) and we'll jump in as soon as we can.

### Testing

If you contribute code, run the appropriate `runtests.py` in each
subproject directory, and please add some tests for your new
contribution.

### Minimal style guide

We generally stick to
[PEP8](http://legacy.python.org/dev/peps/pep-0008/) for our coding
style with the exception of using 2-space tabs. Be sure to indent with
spaces and wrap at 79 characters.

### Submitting code

*Note*: we're currently working through the fact that only repository
owners can follow these directions.  As a temporary measure, use
[edX's rebase
instructions](https://github.com/edx/edx-platform/wiki/How-to-Rebase-a-Pull-Request#fetch-the-latest-version-of-master)
to contribute to this repository.

Before making a contribution, create a new branch off of master:

```
git pull origin master
git checkout -b feature-branch
```

Edit your code and commit it, and then push it back to github:

```
git push origin feature-branch
```

Submit a [pull request](../../compare/), and await a code review.  Once a
reviewer comments 'LGTM' (looks good to me), bring the code into
master with the following command (from your `feature-branch`):

```
./push_branch.sh
```

Many many thanks for contributing!