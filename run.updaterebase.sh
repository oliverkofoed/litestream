#!/bin/bash

echo "Fetching upstream..."
echo "--------------------"
git fetch upstream

echo "Checking out main..."
echo "--------------------"
git checkout main

echo "Rebasing main..."
echo "--------------------"
git rebase upstream/main


echo ""
echo "--------------------"
echo "done, please run 'git push --force' to push the rebased main branch to your fork"
echo "--------------------"