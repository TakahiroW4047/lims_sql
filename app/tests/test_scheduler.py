from freezegun import freeze_time
import unittest
from lib.scheduler import *
from datetime import datetime, timedelta
import pytz
import time


class Test_Dates(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.expected_date = (
            datetime(2020, 1, 1, 10, 30, 0, 0)
            .replace(tzinfo=pytz.UTC)
            .astimezone(pytz.timezone("US/Pacific"))
            )

    @freeze_time('2020-01-01 10:30', tz_offset=-8)
    def test_local_datetime(self):
        mocked_date = local_datetime()
        self.assertEqual(mocked_date, self.expected_date)

    @freeze_time('2020-01-01 10:30', tz_offset=-8)
    def test_local_datetime_string(self):
        mocked_datetime_string = local_datetime_string()
        expected_datetime_string = str(
            datetime.strftime(self.expected_date, '%d%b%y %H:%M')
            .upper()
        )
        self.assertEqual(mocked_datetime_string, expected_datetime_string)


class Test_Scheduler(unittest.TestCase):
    def setUp(self):
        self.result = False

    def test_on(self):
        with freeze_time('2020-01-01 10:30:00', tz_offset=-8) as frozen_time:
            Scheduler([self.sample_function]).on(hour=2, minute=30).run()
            self.assertTrue(self.result==True)
            self.result=False

            next_day = (
            datetime(2020, 1, 2, 10, 30, 0, 0)
            .replace(tzinfo=pytz.UTC)
            .astimezone(pytz.timezone("US/Pacific"))
            )

            frozen_time.move_to(next_day)
            time.sleep(1)
            self.assertTrue(self.result==True)

    def test_every(self):
        with freeze_time('2020-01-01 10:30:00', tz_offset=-8) as frozen_time:
            Scheduler([self.sample_function]).every(hours=0, minutes=30).run()
            time.sleep(1)
            self.assertTrue(self.result==True)
            self.result=False
            frozen_time.tick(delta=timedelta(minutes=30))
            time.sleep(1)
            self.assertTrue(self.result==True)
            self.result=False
            frozen_time.tick(delta=timedelta(minutes=15))
            time.sleep(1)
            self.assertTrue(self.result==False)
            
    def test_on_every(self):
        with freeze_time('2020-01-01 10:00:00', tz_offset=-8) as frozen_time:
            Scheduler([self.sample_function]).on(hour=2, minute=30).every(hours=0, minutes=30).run()
            self.assertTrue(self.result==False)

            expected_initiation_datetime = (
                datetime(2020, 1, 1, 10, 30, 0, 0)
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone("US/Pacific"))
            )

            frozen_time.move_to(expected_initiation_datetime)
            time.sleep(3)
            self.assertTrue(self.result==True)
            self.result=False

            frozen_time.tick(delta=timedelta(minutes=30))
            time.sleep(1)
            self.assertTrue(self.result==True)

    def sample_function(self):
        self.result = True



if __name__ == '__main__':
    unittest.main()