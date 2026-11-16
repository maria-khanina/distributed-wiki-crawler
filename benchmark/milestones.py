"""Prints how long a crawler needed to store each milestone number of pages."""

import time

from benchmark.benchmark_settings import MILESTONES


class MilestoneReporter:
    """Starts a clock on creation and reports each milestone once it is reached."""

    def __init__(self) -> None:
        self.pending: list[int] = sorted(MILESTONES)
        self.start_time = time.monotonic()

    def elapsed_seconds(self) -> float:
        return time.monotonic() - self.start_time

    def report(self, stored_pages: int) -> None:
        while self.pending and stored_pages >= self.pending[0]:
            milestone = self.pending.pop(0)
            print(f"{milestone} pages stored after {self.elapsed_seconds():.1f} s")
