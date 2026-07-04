#!/usr/bin/env python3
"""RQ worker process for Private Video Capture.

Run one instance per desired concurrent download slot (this replaces the
old in-process threading.Semaphore(MAX_CONCURRENT) — concurrency is now
however many of these processes you run, e.g. two systemd instances:
  systemctl enable --now vidapp-worker@1 vidapp-worker@2
"""
import os
os.environ["VIDCAPTURE_WORKER"] = "1"

from rq import Worker
from app import job_queue, rq_redis_conn

if __name__ == "__main__":
    Worker([job_queue], connection=rq_redis_conn).work()
