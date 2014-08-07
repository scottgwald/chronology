#!/usr/bin/env bash

branch=$(git rev-parse --abbrev-ref HEAD 2> /dev/null)

function assert_feature_branch() {
  if [[ "$1" == "HEAD" ]]; then
    echo "You're in some detached state, please switch to a non-dirty branch."
    exit 1
  elif [[ "$1" == "master" ]]; then
    echo "You must be on a feature branch; can't push master."
    exit 1
  elif [[ "$1" == "" ]]; then
    echo "Got a null name for your current branch! Sure you're in a git repo?"
    exit 1
  fi
}

function assert_non_dirty() {
  local status=$(git status --porcelain 2> /dev/null)
  if [[ "$status" != "" ]]; then
    echo "The branch you're on has uncommitted changes; commit or stash them."
    exit 1
  fi
}

function assert_single_arg() {
  if [[ $# -ne 1 ]]; then
    echo "Please provide a single argument to be used as the commit message."
    exit 1
  fi
}

assert_feature_branch "$branch"
assert_non_dirty
assert_single_arg "$@"

git checkout master
echo "> Pulling new changes from origin/master..."
git pull origin master
echo "> Merging $branch into master..."
git merge --squash "$branch"
echo "> Committing..."
git commit -am "$1"
echo "> Pushing commit to origin/master..."
git push origin master
echo "> Cleaning up feature branch..."
git branch -D "$branch"
git push origin :"$branch"
