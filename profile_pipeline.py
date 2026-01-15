"""
Pipeline Profiler for SnowMapper.

Profiles each module for:
- Execution time
- Peak memory usage
- CPU usage

Usage:
    python profile_pipeline.py <module_name> [args...]

Example:
    python profile_pipeline.py compute_basin_stats
    python profile_pipeline.py grid_fsm_to_netcdf ./domains/D2000
"""
import os
import sys
import time
import tracemalloc
import cProfile
import pstats
import io
import psutil
import threading
from datetime import datetime


class ResourceMonitor:
    """Monitor CPU and memory usage in background thread."""

    def __init__(self, interval=0.5):
        self.interval = interval
        self.running = False
        self.peak_memory_mb = 0
        self.peak_cpu_percent = 0
        self.memory_samples = []
        self.cpu_samples = []
        self.thread = None
        self.process = psutil.Process()

    def _monitor(self):
        while self.running:
            try:
                mem = self.process.memory_info().rss / 1024 / 1024  # MB
                cpu = self.process.cpu_percent(interval=None)

                self.memory_samples.append(mem)
                self.cpu_samples.append(cpu)

                if mem > self.peak_memory_mb:
                    self.peak_memory_mb = mem
                if cpu > self.peak_cpu_percent:
                    self.peak_cpu_percent = cpu

                time.sleep(self.interval)
            except:
                break

    def start(self):
        self.running = True
        self.process.cpu_percent()  # First call returns 0, initialize
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)

    def get_stats(self):
        return {
            'peak_memory_mb': self.peak_memory_mb,
            'avg_memory_mb': sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0,
            'peak_cpu_percent': self.peak_cpu_percent,
            'avg_cpu_percent': sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0,
            'samples': len(self.memory_samples)
        }


def profile_module(module_name, *args):
    """Profile a single module with detailed metrics."""

    print(f"\n{'='*60}")
    print(f"PROFILING: {module_name}")
    print(f"{'='*60}\n")

    # Start resource monitor
    monitor = ResourceMonitor(interval=0.5)
    monitor.start()

    # Start memory tracking
    tracemalloc.start()

    # Start CPU profiler
    profiler = cProfile.Profile()

    # Record start time
    start_time = time.time()
    start_mem = psutil.Process().memory_info().rss / 1024 / 1024

    try:
        profiler.enable()

        # Import and run the module
        if module_name == 'compute_basin_stats':
            import compute_basin_stats
            # Module runs on import, or we call main if it exists

        elif module_name == 'grid_fsm_to_netcdf':
            import grid_fsm_to_netcdf
            if args:
                grid_fsm_to_netcdf.main(args[0])
            else:
                print("ERROR: grid_fsm_to_netcdf requires domain path argument")
                return

        elif module_name == 'zonal_stats':
            import zonal_stats
            # Module runs on import

        elif module_name == 'merge_reproject':
            import merge_reproject
            if len(args) >= 2:
                merge_reproject.main(args[0], args[1])
            else:
                print("ERROR: merge_reproject requires sim_dir and domain_dir arguments")
                return

        elif module_name == 'run_forecast_sim':
            import run_forecast_sim
            if args:
                run_forecast_sim.main(args[0])
            else:
                print("ERROR: run_forecast_sim requires domain path argument")
                return
        else:
            print(f"ERROR: Unknown module {module_name}")
            return

        profiler.disable()

    except Exception as e:
        profiler.disable()
        print(f"ERROR during profiling: {e}")
        traceback_str = traceback.format_exc()
        print(traceback_str)

    # Stop monitoring
    monitor.stop()
    end_time = time.time()

    # Get memory stats
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    resource_stats = monitor.get_stats()

    # Print results
    print(f"\n{'='*60}")
    print(f"PROFILING RESULTS: {module_name}")
    print(f"{'='*60}")

    print(f"\n⏱️  TIMING:")
    print(f"   Total execution time: {end_time - start_time:.2f}s")

    print(f"\n💾 MEMORY:")
    print(f"   Start memory:     {start_mem:.1f} MB")
    print(f"   Peak memory:      {resource_stats['peak_memory_mb']:.1f} MB")
    print(f"   Avg memory:       {resource_stats['avg_memory_mb']:.1f} MB")
    print(f"   Tracemalloc peak: {peak / 1024 / 1024:.1f} MB")

    print(f"\n🔥 CPU:")
    print(f"   Peak CPU:         {resource_stats['peak_cpu_percent']:.1f}%")
    print(f"   Avg CPU:          {resource_stats['avg_cpu_percent']:.1f}%")
    print(f"   Samples:          {resource_stats['samples']}")

    # Top functions by time
    print(f"\n📊 TOP 15 FUNCTIONS BY TIME:")
    s = io.StringIO()
    stats = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    stats.print_stats(15)
    print(s.getvalue())

    # Top functions by calls
    print(f"\n📊 TOP 10 FUNCTIONS BY CALLS:")
    s = io.StringIO()
    stats = pstats.Stats(profiler, stream=s).sort_stats('calls')
    stats.print_stats(10)
    print(s.getvalue())

    return {
        'module': module_name,
        'time_seconds': end_time - start_time,
        'peak_memory_mb': resource_stats['peak_memory_mb'],
        'peak_cpu_percent': resource_stats['peak_cpu_percent'],
    }


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    module_name = sys.argv[1]
    args = sys.argv[2:]

    profile_module(module_name, *args)


if __name__ == "__main__":
    main()
