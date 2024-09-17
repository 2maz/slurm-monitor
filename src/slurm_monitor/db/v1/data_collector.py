from argparse import ArgumentParser
import subprocess
from pathlib import Path
import sys
from threading import Thread
import time
import datetime as dt
from abc import abstractmethod
from queue import Queue, Empty
from typing import Generic, TypeVar

import logging
import re

from slurm_monitor.utils import utcnow
from .db import SlurmMonitorDB, DatabaseSettings, Database
from .db_tables import GPUStatus, JobStatus, Nodes

from slurm_monitor.slurm import Slurm

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

    def __init__(self, db: Database, name: str = ''):
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
                print(f"{self.name} queue size: {self._samples.qsize()} {utcnow()} (UTC)\r", end="")
                time.sleep(5)
        [x.thread.join() for x in self._collectors]

    def save(self, samples):
        self.db.insert_or_update(samples)

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

def cli_run():
    Slurm.ensure_restd()

    parser = ArgumentParser()
    parser.add_argument("mode", default="prod")
    parser.add_argument("--db_uri", type=str, help="timescaledb://slurmuser:ex3cluster@srl-login3.cm.cluster:10100/ex3cluster")

    args, unknown = parser.parse_known_args()

    db_settings = DatabaseSettings()
    db_settings.uri = args.db_uri

    if db_settings.uri.startswith("sqlite:///"):
        db_home = Path(db_settings.uri.replace("sqlite:///","")).parent
        db_home.mkdir(parents=True, exist_ok=True)

    if args.mode == "dev":
        db_settings.uri = db_settings.uri.replace(".sqlite", ".dev.sqlite")
        logger.warning(f"Running in development mode: using {db_settings.uri}")
    elif args.mode == "prod":
        logger.warning(f"Running in production mode: using {db_settings.uri}")
    else:
        logger.warning("Missing 'mode'")
        print(parser)
        sys.exit(10)

    run(db_settings)

def run(db_settings: DatabaseSettings | None = None):
    if db_settings is None:
        db_settings = DatabaseSettings()

    db = SlurmMonitorDB(db_settings=db_settings)

    job_status_collector = JobStatusCollector()
    jobs_pool = CollectorPool[JobStatus](db=db, name='job status')
    jobs_pool.add_collector(job_status_collector)
    jobs_pool.start()

    while True:
        answer = input("\nEnter 'q' to quit\n\n")
        if answer.lower().startswith('q'):
            break

    jobs_pool.stop()
