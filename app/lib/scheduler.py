from datetime import datetime, timedelta
import logging
import pytz
import threading
import time
import unittest

class Scheduler:
    def __init__(self, func_list):
        self.func_list = func_list
        self.status_on_used = False
        self.status_every_used = False

    def on(self, hour, minute):
        self.time_on = local_datetime().replace(hour=hour, minute=minute)
        self.status_on_used = True
        return self

    def every(self, hours, minutes):
        self.time_every = timedelta(hours=hours, minutes=minutes)
        self.status_every_used = True
        return self

    def func_on(self, func):
            has_ran = False
            while True:
                time.sleep(1)
                timenow = local_datetime()
                if (timenow==self.time_on) and has_ran==False:
                    start_time = datetime.now()
                    func()
                    delta = datetime.now() - start_time
                    logging.info(f"{local_datetime_string()}: {func.__name__} Task Completed. Duration: {delta}")
                    has_ran = True
                if (timenow!=self.time_on):
                    has_ran = False 

    def func_every(self, func):
            initiated = False
            while initiated==False:
                time.sleep(1)
                timenow = local_datetime()
                if (self.time_on==timenow and initiated==False):
                    initiated = True

            has_ran = False
            timenext = local_datetime() + self.time_every
            start_time = datetime.now()
            func()
            delta = datetime.now() - start_time
            logging.info(f"{local_datetime_string()}: {func.__name__} Task Completed. Duration: {delta}")

            while True:
                time.sleep(1)
                timenow = local_datetime()
                if (timenow==timenext and has_ran==False):
                    start_time = datetime.now()
                    func()
                    delta = datetime.now() - start_time
                    logging.info(f"{local_datetime_string()}: {func.__name__} Task Completed. Duration: {delta}")
                    has_ran=True
                    timenext += self.time_every
                if (timenow!=timenext):
                    has_ran=False

    def run(self):
        if self.status_on_used==False:
            self.time_on = local_datetime()
        if self.status_every_used==False:
            for func in self.func_list:
                thread = threading.Thread(target=self.func_on, args=(func,))
                thread.start()
        else:
            for func in self.func_list:
                thread = threading.Thread(target=self.func_every, args=(func,))
                thread.start()

        
def local_datetime():
    dt = datetime.utcnow().replace(second=0, microsecond=0)
    return datetime_utc_to_local(dt)

def local_datetime_string():
    dt = datetime_utc_to_local(datetime.utcnow())
    dt_string = str(
        datetime.strftime(dt, "%d%b%y %H:%M")
        ).upper()
    return dt_string

def datetime_utc_to_local(dt):
    date = dt.replace(tzinfo=pytz.UTC)
    date = date.astimezone(pytz.timezone("US/Pacific"))
    return date

if __name__ == '__main__':
    def func_test():
        timenow = datetime.now()
        print(timenow)
        return None

    test = Scheduler([func_test]).every(hours=0, minutes=1).run()