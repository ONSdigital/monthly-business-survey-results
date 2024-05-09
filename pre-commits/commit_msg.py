#!/usr/bin/env python3
"""Git hook to check git commit message has appropriate length subject line.

After removing the jira issue number from the subject line we check that the
message is longer than 20 characters and shorter than 65.
"""
import sys

# Collect the parameters
commit_msg_filepath = sys.argv[1]

with open(commit_msg_filepath, "r") as f:
    lines = f.readlines()

    # The subject is the first line of the message, but we don't count any
    # Jira issue note
    commit_subject = lines[0].split("]")[-1]

    if len(commit_subject) < 20:
        print(
            f"""
            commit-msg: ERROR! The commit subject is too short!
            subject length = {len(commit_subject)} < 20 characters'
            """
        )
        sys.exit(1)

    elif len(commit_subject) > 65:
        # We check if messages are greater than 65 char, but warn as if
        # longer than 50
        print(
            f"""
            commit-msg: ERROR!
            The commit subject is too long!
            subject length = {len(commit_subject)} > 50 characters'
            """
        )
        sys.exit(1)

#    for line in lines[2:]:
#        print(line)
