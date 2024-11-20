#! /usr/bin/env python

import sys
import os
import tempfile
import logging
import fcntl

MAVEN_TMPDIR = '/locks/maven/'
TMPDIR_ENV = 'TMPDIR'


class SingleInstance:
    """
    If you want to prevent your script from running in parallel just instantiate SingleInstance() class.
    If there is another instance already running it will exit the application with the message "Another
    instance is already running, quitting.", returning -1 error code.

    >>> import tendo
    ... me = SingleInstance()

    This option is very useful if you have scripts executed by crontab at small amounts of time.

    Remember that this works by creating a lock file with a filename based on the full path to the script file.
    """

    def __init__(self, flavor_id=""):
        self.initialized = False
        basename = os.path.splitext(os.path.abspath(sys.argv[0]))[0].replace("/", "-").replace(":", "").replace("\\", "-") + '-%s' % flavor_id + '.lock'
        # os.path.splitext(os.path.abspath(sys.modules['__main__'].__file__))[0].replace("/", "-").replace(":", "")\
        # .replace("\\", "-") + '-%s' % flavor_id + '.lock'
        original_TMPDIR = os.environ.get(TMPDIR_ENV, None)
        os.environ[TMPDIR_ENV] = MAVEN_TMPDIR
        self.lockfile = os.path.normpath(tempfile.gettempdir() + '/' + basename)
        if original_TMPDIR:
            os.environ[TMPDIR_ENV] = original_TMPDIR
        else:
            os.environ.pop(TMPDIR_ENV)

        logger.debug("SingleInstance lockfile: " + self.lockfile)
        # non Windows
        self.fp = open(self.lockfile, 'w')
        try:
            fcntl.lockf(self.fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            # logger.warning("Another instance is already running, quitting.")
            sys.exit(-1)
        self.initialized = True

    def __del__(self):
        if not self.initialized:
            return
        fcntl.lockf(self.fp, fcntl.LOCK_UN)
        # os.close(self.fp)
        if os.path.isfile(self.lockfile):
            os.unlink(self.lockfile)

logger = logging.getLogger("tendo.singleton")
logger.addHandler(logging.StreamHandler())
