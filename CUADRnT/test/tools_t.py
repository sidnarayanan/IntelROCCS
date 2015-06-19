#!/usr/bin/env python
"""
File       : dataset_collector_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for dataset collector class
"""

# system modules
import unittest

# package modules
from UADR.tools.dataset_collector import DatasetCollector
from UADR.utils.config import get_config

# @unittest.skip("Skipping Test")
class ServicesTests(unittest.TestCase):
    """
    A test class for service classes
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(config='cuadrnt-test.cfg')

    def tearDown(self):
        "Clean up"
        pass

    def test_dataset_collector(self):
        "test dataset_collector functions"
        print ""
        dataset_collector = DatasetCollector(config=self.config)
        dataset_collector.get_data()

if __name__ == '__main__':
    unittest.main()
