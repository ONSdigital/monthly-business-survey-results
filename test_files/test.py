import argparse
import json
import math
import os
from typing import Optional, Sequence, Set


def count(numbers):
    total = 0
    for x in numbers:

        total += x
    return total


count([2, 4, 6, 8])
