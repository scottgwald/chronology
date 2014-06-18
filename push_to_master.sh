#!/usr/bin/env bash

function git_branch() {
  # Based on: http://stackoverflow.com/a/13003854/170413
  local branch
  if branch=$(git rev-parse --abbrev-ref HEAD 2> /dev/null); then
    if [[ "$branch" == "HEAD" ]]; then
      echo "You're in some detached state, please switch to a non-dirty branch."
      exit 1
    elif [[ "$branch" == "master" ]]; then
      echo "You must be on a feature branch; can't push master."
      exit 1
    fi 
 else
    echo "Failed to find the name of the current branch!"
    exit 1
  fi
  echo "$branch"
}

function assert_non_dirty() {
  local status=$(git status --porcelain 2> /dev/null)
  if [[ "$status" != "" ]]; then
    echo "The branch you're on has uncommitted changes; commit or stash them."
    exit 1
  fi
}

function assert_single_arg() {
  if (( $# != 1 )); then
    echo "Please provide a single argument to be used as the commit message."
    exit 1
  fi
}

assert_non_dirty
assert_single_arg

branch=$(git_branch)
git checkout master
echo "> Pulling new changes from origin/master..."
git pull origin master
echo "> Merging $branch into master..."
git merge --squash "$branch"
echo "> Committing..."
git commit -am "$1"
echo "> Pushes commit to origin/master..."
git push origin master
