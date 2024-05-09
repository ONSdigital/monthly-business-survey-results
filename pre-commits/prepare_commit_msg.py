#!/usr/bin/env python3
"""Git hook to automatically prefix git commit message with Jira issue number.

The issue number (e.g. Jira ticket number) from the current branch name. Works
with or without specifying -m option at commit time.
"""
import re
import sys
from subprocess import check_output

commit_msg_filepath = sys.argv[1]
branch = (
    check_output(["git", "symbolic-ref", "--short", "HEAD"]).decode("utf-8").strip()
)

# If branch name contains /'s we only want the final part of the branch name
branch_end = branch.split("/")[-1]

# Regex pattern for matching to Jira issues
regex = r"[Jj]\d+"

if re.search(regex, branch_end):
    # Create list of all matches to regex pattern
    issue_number_matches = re.findall(regex, branch_end)

    # If mutiple issues in branch name we join them together
    commit_issue = f'{"_".join(issue_number_matches)}'

    with open(commit_msg_filepath, "r+") as f:
        commit_msg = f.read()
        f.seek(0, 0)  # correctly position issue_number when writing commit msg
        f.write(f"[{commit_issue}] {commit_msg}")

else:
    # If branch does not contain a jira issue number, reject the commit
    print(
        f"""
        prepare-commit-msg: Error!
        Branch name is {branch}
        Does not match branch name strategy \'*/jxxx\'
        """
    )
    sys.exit(1)
