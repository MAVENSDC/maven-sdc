'''
Created on Oct 27, 2016

@author: bstaley
'''
import os
import unittest
import sys
from shutil import rmtree

from maven_utilities import time_utilities, utilities as util_utilities
from tests.maven_test_utilities.file_system import get_temp_root_dir
from tests.maven_test_utilities.file_names import generate_science_file_name
from maven_file_cleaner import utilities


class TestFileCleaner(unittest.TestCase):

    def setUp(self):
        self.test_root = get_temp_root_dir()

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))

    def testBuildVersionRevisionDataNonRecursive(self):
        test_roots = ['foo', 'bar']
        child_roots = ['fooc', 'barc']
        root_files = []

        for _next in test_roots:
            root_files.extend(self.build_data(test_dir=self.test_root,
                                              file_root=_next,
                                              max_ver=3,
                                              max_rev=3))

        # add some files to sub-dir
        for _next in child_roots:
            self.build_data(test_dir=os.path.join(self.test_root, 'child'),
                            file_root=_next,
                            max_ver=3,
                            max_rev=3)

        results = utilities.build_version_revision_data(self.test_root, recursive=False)

        self.check_vr_data(vr_data=results,
                           truth_roots=test_roots,
                           truth_file_list=root_files)

    def testBuildVersionRevisionDataRecursive(self):
        test_roots = ['foo', 'bar']
        child_roots = ['fooc', 'barc']
        root_files = []

        for _next in test_roots:
            root_files.extend(self.build_data(test_dir=self.test_root,
                                              file_root=_next,
                                              max_ver=11,  # > 10 to test 2 digit ver/rev
                                              max_rev=11))

        # add some files to sub-dir
        for _next in child_roots:
            root_files.extend(self.build_data(test_dir=os.path.join(self.test_root, 'child'),
                                              file_root=_next,
                                              max_ver=3,
                                              max_rev=3))

        results = utilities.build_version_revision_data(self.test_root, recursive=True)

        self.check_vr_data(vr_data=results,
                           truth_roots=test_roots + child_roots,
                           truth_file_list=root_files)

    def testGetLatestVrData(self):
        from collections import OrderedDict

        def get_truth_files(from_vr_data, file_root, max_v=None, max_r=None, least_v=None, least_r=None):
            results = set()
            for k, vs in from_vr_data.items():
                if k != file_root:
                    continue
                for kv, rs in vs.items():
                    if (least_v and kv < least_v) or (max_v and kv > max_v):
                        continue
                    for r, pn in rs.items():
                        if (least_r and r < least_r) or (max_r and r > max_r):
                            continue
                        results.add(pn)
            return results

        # Generate test vr data in {root:{ver:{rev:pn}}}
        test_data = {}
        test_roots = ['foo', 'bar']
        truth_all = {}
        for next_root in test_roots:
            for next_v in range(3, 0, -1):
                for next_r in range(3, 0, -1):
                    test_data.setdefault(next_root, OrderedDict()).setdefault(next_v, OrderedDict()).setdefault(next_r, '%s_v%s_r%s' % (next_root, next_v, next_r))
            truth_all[next_root] = get_truth_files(test_data, next_root)

        # test keep all vr data
        results = utilities.get_latest_version_revision_data(v_r_data=test_data,
                                                             num_versions_to_keep=sys.maxsize,
                                                             num_revisions_to_keep=sys.maxsize)
        self.assertEqual(set(test_roots), set(results.keys()))

        for next_root in test_roots:
            self.assertEqual(truth_all[next_root], set(results[next_root][0]))  # test newest
            self.assertEqual(set(), set(results[next_root][1]))  # test oldest

        # test keep no vr data
        results = utilities.get_latest_version_revision_data(v_r_data=test_data,
                                                             num_versions_to_keep=0,
                                                             num_revisions_to_keep=0)
        self.assertEqual(set(test_roots), set(results.keys()))

        for next_root in test_roots:
            truth_newest = set()
            truth_oldest = truth_all[next_root] - truth_newest
            self.assertEqual(truth_newest, set(results[next_root][0]))  # test newest
            self.assertEqual(truth_oldest, set(results[next_root][1]))  # test oldest

        # test keep some vr data
        results = utilities.get_latest_version_revision_data(v_r_data=test_data,
                                                             num_versions_to_keep=1,
                                                             num_revisions_to_keep=1)
        self.assertEqual(set(test_roots), set(results.keys()))

        for next_root in test_roots:
            truth_newest = get_truth_files(test_data, next_root, 3, 3, 3, 3)
            truth_oldest = truth_all[next_root] - truth_newest

            self.assertEqual(truth_newest, set(results[next_root][0]))  # test newest
            self.assertEqual(truth_oldest, set(results[next_root][1]))  # test oldest

    def testCleanDirectoryNonRecursive(self):

        self.build_data(test_dir=self.test_root,
                        file_root='test',
                        max_ver=3,
                        max_rev=3)

        # add some files to sub-dir
        self.build_data(test_dir=os.path.join(self.test_root, 'child'),
                        file_root='child',
                        max_ver=3,
                        max_rev=3)

        utilities.clean_directory(directory=self.test_root,
                                  recursive=False,
                                  num_versions_to_keep=2,
                                  num_revisions_to_keep=2,
                                  dry_run=False)

        self.check_files_in_dir(directory=self.test_root, min_v=1, min_r=1, recursive=False)

    def testCleanDirectoryRecursive(self):

        self.build_data(test_dir=self.test_root,
                        file_root='test',
                        max_ver=3,
                        max_rev=3)

        # add some files to sub-dir
        self.build_data(test_dir=os.path.join(self.test_root, 'child'),
                        file_root='child',
                        max_ver=3,
                        max_rev=3)

        utilities.clean_directory(directory=self.test_root,
                                  recursive=True,
                                  num_versions_to_keep=2,
                                  num_revisions_to_keep=2,
                                  dry_run=False)

        self.check_files_in_dir(directory=self.test_root, min_v=1, min_r=1, recursive=True)

    def testCleanDirectoryDryrn(self):

        self.build_data(test_dir=self.test_root,
                        file_root='test',
                        max_ver=3,
                        max_rev=3)

        # add some files to sub-dir
        self.build_data(test_dir=os.path.join(self.test_root, 'child'),
                        file_root='child',
                        max_ver=3,
                        max_rev=3)

        utilities.clean_directory(directory=self.test_root,
                                  recursive=True,
                                  num_versions_to_keep=2,
                                  num_revisions_to_keep=2,
                                  dry_run=True)

        self.check_files_in_dir(directory=self.test_root, min_v=0, min_r=0, recursive=True)

    def check_files_in_dir(self, directory, min_v, min_r, recursive=False):
        for pn in util_utilities.listdir_files(directory=directory, recursive=recursive):
            _, ver, rev = util_utilities.get_file_root_plus_extension_with_version_and_revision(os.path.basename(pn))
            if ver < min_r or rev < min_r:
                self.fail('Found a version/revision < then minimum version revision v %s r %s mv %s mr %s' % (ver, rev, min_v, min_r))

    def check_vr_data(self, vr_data, truth_roots, truth_file_list):

        self.assertEqual(len(truth_roots), len(vr_data))

        # ensure descending order
        for key in vr_data:
            running_ver = sys.maxsize
            for ver in vr_data[key]:
                running_rev = sys.maxsize
                for rev in vr_data[key][ver]:
                    self.assertLessEqual(rev, running_rev)
                    running_rev = rev
                self.assertLessEqual(ver, running_ver)
                running_ver = ver

        test_results = []
        for key in vr_data:
            for ver in vr_data[key]:
                for rev in vr_data[key][ver]:
                    test_results.append(vr_data[key][ver][rev])

        self.assertEqual(set(truth_file_list), set(test_results))

    def build_data(self, test_dir, file_root, max_ver, max_rev):
        from tests.maven_test_utilities.file_system import build_test_files_and_structure

        file_time = time_utilities.utc_now()
        files = []

        for next_v in range(max_ver):
            for next_r in range(max_rev):
                files.append(generate_science_file_name(description=file_root,
                                                        file_dt=file_time,
                                                        version=next_v,
                                                        revision=next_r))

        build_test_files_and_structure(default_file_contents='this is only a test',
                                       files_base_dir=test_dir,
                                       files_list=files)

        # return fully qualified names
        return [os.path.join(test_dir, x) for x in files]
