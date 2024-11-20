"""
Created on Sep 15 2020

This modules provides MAVEN SDC enums:

@author: Bryan Staley
"""
from enum import Enum


class DUPLICATE_ACTION(Enum):
    OVERWRITE = 1  # Overwrite the destination file
    OVERWRITE_ARCHIVE = 2  # Overwrite the destination file, archive the destination file
    REMOVE = 3  # Remove the source file
    ARCHIVE = 4  # Archive and remove the source file
    IGNORE = 5  # Do nothing
    UP_VERSION=6
