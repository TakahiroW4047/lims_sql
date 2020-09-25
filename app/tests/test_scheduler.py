import unittest
from lib.scheduler import Scheduler
from datetime import datetime

class MockDate(datetime):
    @classmethod
    def now(cls):
        return cls(year=2020, month=1, day=1, hour=10, minute=30)

class Test_Scheduler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        scheduler = Scheduler(func_mock)

    def test_on(self):
        datetime = MockDate(2000, 1, 1, 0, 0)
        func_mock()


def func_mock():
    print(datetime.now())
    return "Test String"


if __name__ == '__main__':
    unittest.main()