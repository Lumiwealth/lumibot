from time import perf_counter


class PerfCounters:
    counters: dict[str, list[float]]

    def __init__(self) -> None:
        self.counters = {}

    def add_counter(self, name: str) -> None:
        self.counters[name] = [0, 0]

    def tic_counter(self, name: str) -> None:
        self.counters[name][1] = perf_counter()

    def toc_counter(self, name: str) -> None:
        toc = perf_counter()
        counter = self.counters[name]
        tic = counter[1]
        counter[0] += toc - tic
        self.counters[name] = counter


perf_counters = PerfCounters()
