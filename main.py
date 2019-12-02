from functools import partial, update_wrapper
import datetime
import time


class ScheduleError(Exception):
	pass

class ScheduleValueError(ScheduleError):
	pass

class IntervalError(ScheduleValueError):
	pass



class Scheduler:
	def __init__(self):
		self.jobs = []

	def every(self, interval=1):
		job = Job(interval)
		self.jobs.append(job)
		return job

	def run_pending(self):
		all_jobs = (job for job in self.jobs if job.should_run)
		for job in sorted(all_jobs):
			job.run()

	def run_all(self, delay_seconds):
		for job in self.jobs:
			job.run()
			time.sleep(delay_seconds)

	@property
	def next_run(self):
		if not self.jobs:
			return None
		return min(self.jobs).next_run

	@property
	def idle_seconds(self):
		return (self.next_run - datetime.datetime.now()).total_seconds()


class Job:
	def __init__(self, interval):
		self.interval = interval
		self.job_func = None
		self.last_run = None
		self.next_run = None
		self.at_time = None
		self.unit = None
		self.period = None

	def __lt__(self, other):
		return self.next_run < other.next_run

	@property
	def second(self):
		if self.interval != 1:
			raise IntervalError('use seconds instead of second')
		return self.seconds

	@property
	def seconds(self):
		self.unit = 'seconds'
		return self

	@property
	def minute(self):
		if self.interval != 1:
			raise IntervalError('use minutes instead of minute')
		return self.minutes

	@property
	def minutes(self):
		self.unit = 'minutes'
		return self

	@property
	def hour(self):
		if self.interval != 1:
			raise IntervalError('use hours instead of hour')
		return self.hours

	@property
	def hours(self):
		self.unit = 'hours'
		return self

	@property
	def day(self):
		if self.interval != 1:
			raise IntervalError('use days instead of day')
		return self.days

	@property
	def days(self):
		self.unit = 'days'
		return self


	def do(self, job_func, *args, **kwargs):
		self.job_func = partial(job_func, *args, **kwargs)
		update_wrapper(self.job_func, job_func)
		self._schedule_next_run()
		return self

	def at(self, time_str):
		if self.unit not in ('hours', 'days'):
			raise ScheduleValueError('Invalid unit')
		hour, minute = [t for t in time_str.split(':')]
		minute = int(minute)
		if not 0 < minute < 59:
			raise ScheduleValueError('Invalid minute')
		if self.unit == 'days':
			hour = int(hour)
			if not 0 < hour < 23:
				raise ScheduleValueError('Invalid hour')
		elif self.unit == 'hours':
			hour = 0
		self.at_time = datetime.time(hour=hour, minute=minute)
		return self


	def _schedule_next_run(self):
		if self.unit not in ('seconds', 'minutes', 'hours', 'days'):
			raise ScheduleValueError('Invalid unit')
		self.period = datetime.timedelta(**{self.unit:self.interval})
		self.next_run = datetime.datetime.now() + self.period
		if self.at_time is not None:
			if self.unit not in ('hours', 'days'):
				raise ScheduleValueError('Invalid unit')
			kwargs = {
				'minute': self.at_time.minute,
				'second': 0,
				'microsecond': 0,
			}
			if self.unit == 'days':
				kwargs['hour'] = self.at_time.hour
			self.next_run = self.next_run.replace(**kwargs)
			if not self.last_run:
				now = datetime.datetime.now()
				if self.unit == 'days' and self.at_time > now.time():
					self.next_run = self.next_run - datetime.timedelta(days=1)
				elif self.unit == 'hours' and self.at_time.minute > now.minute:
					self.next_run = self.next_run - datetime.timedelta(hours=1)

	def run(self):
		ret = self.job_func()
		self.last_run = datetime.datetime.now()
		self._schedule_next_run()
		return ret

	@property
	def should_run(self):
		return datetime.datetime.now() >= self.next_run


default_scheduler = Scheduler()


def every(interval=1):
	return default_scheduler.every(interval)


def run_pending():
	return default_scheduler.run_pending()

def run_all(delay_seconds=0):
	default_scheduler.run_all(delay_seconds)

def next_run():
	return default_scheduler.next_run

def idle_seconds():
	return default_scheduler.idle_seconds

