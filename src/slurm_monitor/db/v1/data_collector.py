import subprocess
from threading import Thread
import time
import datetime as dt
from queue import Queue, Empty
from typing import Generic, TypeVar

import logging

from slurm_monitor.utils import utcnow
from slurm_monitor.utils.slurm import Slurm

from slurm_monitor.db.v1.db import SlurmMonitorDB, Database
from slurm_monitor.db.v1.db_tables import JobStatus


logger = logging.getLogger(__name__)

T = TypeVar("T")


class Observer(Generic[T]):
    _samples: Queue

    def __init__(self):
        self._samples = Queue()

    def notify(self, samples: list[T]):
        [self._samples.put(x) for x in samples]


class Observable(Generic[T]):
    observers: list[Observer[T]]

    def __init__(self, observers: list[Observer[T]] = []):
        self.observers = observers

    def add_observer(self, observer: Observer[T]):
        if observer not in self.observers:
            self.observers.append(observer)

    def notify_all(self, samples: list[T]):
        [x.notify(samples) for x in self.observers]


class DataCollector(Observer[T]):
    thread: Thread
    _stop: bool = False

    # adapts dynamically
    sampling_interval_in_s: int
    # for resetting
    _sampling_interval_in_s: int

    name: str

    def __init__(self, name: str, sampling_interval_in_s: int):
        self.name = name
        self.sampling_interval_in_s = sampling_interval_in_s
        self._sampling_interval_in_s = sampling_interval_in_s
        self.thread = Thread(target=self._run, args=())

    @staticmethod
    def get_user():
        return (
            subprocess.run("whoami", stdout=subprocess.PIPE)
            .stdout.decode("utf-8")
            .strip()
        )

    def start(self):
        self._stop = False
        self.thread.start()

    def _run(self):
        start_time = None
        while not self._stop:
            if (
                start_time
                and (dt.datetime.now() - start_time).total_seconds()
                < self.sampling_interval_in_s
            ):
                time.sleep(1)
            else:
                try:
                    samples: list[T] = self.run()
                    self.notify_all(samples=samples)

                    # reset sampling interval upon successful retrieval
                    self.sampling_interval_in_s = self._sampling_interval_in_s
                except Exception as e:
                    # Dynamically increase the sampling interval for failing nodes, but cap at
                    # 15 min
                    self.sampling_interval_in_s = min(
                        self.sampling_interval_in_s * 2, 15 * 60
                    )
                    logger.warning(
                        f"{self.name}: failed to collect data."
                        f" Increasing sampling interval to {self.sampling_interval_in_s} s."
                        f" -- details: {e}"
                    )

                start_time = dt.datetime.now()

    def stop(self):
        self._stop = True

    def join(self):
        self.thread.join()

    def notify(self, data: list[T]):
        pass


class CollectorPool(Generic[T], Observer[T]):
    name: str
    _collectors: list[DataCollector[T]]
    db: Database
    _stop: bool = False
    verbose: bool = True

    def __init__(self, db: Database, name: str = ""):
        super().__init__()

        self.name = name
        self._collectors = []
        self.monitor_thread = Thread(target=self._run, args=())
        self.save_thread = Thread(target=self._save, args=())

        self.db = db

    def add_collector(self, collector: DataCollector[T]):
        if collector not in self._collectors:
            collector.add_observer(self)
            self._collectors.append(collector)

    def _run(self):
        [x.start() for x in self._collectors]
        if self.verbose:
            while not self._stop:
                print(
                    f"{self.name} queue size: {self._samples.qsize()} {utcnow()} (UTC)\r",
                    end="",
                )
                time.sleep(5)
        [x.thread.join() for x in self._collectors]

    def save(self, samples):
        prep_samples = []
        for s in samples:
            if isinstance(s, JobStatus) and s.job_state != "RUNNING":
                job_status = self.db.fetch_first(
                    JobStatus,
                    where=(JobStatus.job_id == s.job_id)
                    & (JobStatus.submit_time == s.submit_time),
                )
                if job_status is not None:
                    s.gres_detail = job_status.gres_detail
            prep_samples.append(s)

        self.db.insert_or_update(prep_samples)

    def _save(self, batch_size: int = 500):
        while not self._stop:
            samples = []
            try:
                for i in range(0, batch_size):
                    sample: T = self._samples.get(block=False)
                    samples.append(sample)
            except Empty:
                pass

            try:
                if samples:
                    self.save(samples)
            except Exception as e:
                logger.warning(f"Error on save -- {e}")

        # Finalize saving of sample after stop
        samples = []
        while True:
            try:
                samples.append(self._samples.get(block=False))
            except Empty:
                if samples:
                    self.save(samples)
                break
        assert self._samples.qsize() == 0

    def start(self, verbose: bool = True):
        self._stop = False
        self.verbose = verbose

        self.monitor_thread.start()
        self.save_thread.start()

    def stop(self):
        self._stop = True
        [x.stop() for x in self._collectors]

    def join(self):
        self.monitor_thread.join()
        self.save_thread.join()


class JobStatusCollector(DataCollector[JobStatus], Observable[JobStatus]):
    user: str = None

    def __init__(self, user: str = None):
        super().__init__(name="job-info-collector", sampling_interval_in_s=30)
        self.observers = []

        if user is None:
            self.user = self.get_user()

    def run(self) -> list[JobStatus]:
        response = Slurm.get_slurmrestd("/jobs")

        if response == "" or response is None:
            raise ValueError("JobsCollector: No value response")

        return [JobStatus.from_json(x) for x in response["jobs"]]


def start_jobs_collection(
    database: SlurmMonitorDB | None = None,
) -> CollectorPool[JobStatus]:
    job_status_collector = JobStatusCollector()
    jobs_pool = CollectorPool[JobStatus](db=database, name="job status")
    jobs_pool.add_collector(job_status_collector)
    jobs_pool.start()
    return jobs_pool
