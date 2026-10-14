"""
Some pipelines used for testing
"""

import time


class DelayPipeline:
    delay = 0.05

    def process_item(self, item):
        time.sleep(self.delay)
        return item


class ZeroDivisionErrorPipeline:
    def open_spider(self):
        1 / 0

    def process_item(self, item):
        return item


class ProcessWithZeroDivisionErrorPipeline:
    def process_item(self, item):
        1 / 0
