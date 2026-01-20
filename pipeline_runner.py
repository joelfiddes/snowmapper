"""
Pipeline Runner for SnowMapper.

Handles orchestration, logging, timing, and status tracking for pipeline steps.
Keeps shell script simple - just a list of modules to run.

Usage:
    from pipeline_runner import Pipeline

    with Pipeline() as p:
        p.run("Fetch IFS forecast", "fetch_ifs_forecast.py")
        p.run("Download ERA5", "download_era5.py")
        p.skip_if(archive_exists, "Init domain", "init_domain.py", domain)
        ...
"""
import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path
from config import load_config, get_enabled_domains


class Pipeline:
    """Pipeline orchestrator with logging, timing, and status tracking."""

    def __init__(self, sim_dir=None):
        self.sim_dir = Path(sim_dir or os.getcwd()).resolve()
        self.cfg = load_config(str(self.sim_dir))
        self.scripts_dir = Path(self.cfg['paths']['snowmapper_scripts'])
        self.logs_dir = Path(self.cfg['paths']['logs_dir'])
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Logging
        self.log_file = self.logs_dir / "pipeline.log"
        self.summary_file = self.logs_dir / "pipeline_summary.txt"

        # Tracking
        self.steps = []  # List of (name, status, duration)
        self.start_time = None
        self.failed = False

    def __enter__(self):
        self.start_time = time.time()
        self._clear_log()
        self._log_header()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._print_summary()
        self._log_footer()
        return False  # Don't suppress exceptions

    def _clear_log(self):
        self.log_file.write_text("")

    def _log(self, msg):
        line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}"
        print(line)
        with open(self.log_file, 'a') as f:
            f.write(line + "\n")

    def _log_header(self):
        self._log("=" * 50)
        self._log("SnowMapper Pipeline Started")
        self._log(f"Sim directory: {self.sim_dir}")
        self._log(f"Scripts: {self.scripts_dir}")
        domains = [d['name'] for d in get_enabled_domains(self.cfg)]
        self._log(f"Domains: {', '.join(domains)}")
        self._log("=" * 50)

    def _log_footer(self):
        elapsed = time.time() - self.start_time
        hours, remainder = divmod(int(elapsed), 3600)
        minutes, seconds = divmod(remainder, 60)
        runtime = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        self._log("=" * 50)
        self._log("Pipeline Complete" if not self.failed else "Pipeline Failed")
        self._log(f"Total runtime: {runtime}")
        self._log(f"Summary: {self.summary_file}")
        self._log("=" * 50)

    def _format_duration(self, seconds):
        if seconds == 0:
            return "--"
        mins, secs = divmod(int(seconds), 60)
        return f"{mins}m {secs:02d}s"

    def _clear_swap(self):
        """Clear swap to free memory between modules (requires sudo)."""
        if not self.cfg.get('pipeline', {}).get('clear_swap', False):
            return
        try:
            subprocess.run(
                ["sudo", "swapoff", "-a"],
                capture_output=True, timeout=60
            )
            subprocess.run(
                ["sudo", "swapon", "-a"],
                capture_output=True, timeout=60
            )
        except Exception:
            pass  # Silently ignore if swap clearing fails

    def _print_summary(self):
        lines = []
        lines.append("=" * 58)
        lines.append(" PIPELINE SUMMARY")
        lines.append("=" * 58)
        lines.append("")
        lines.append(f"{'Status':<8} {'Module':<38} {'Duration':>10}")
        lines.append(f"{'─'*6:<8} {'─'*38:<38} {'─'*10:>10}")

        completed = skipped = failed = 0

        for name, status, duration in self.steps:
            symbol = {"completed": "✓", "skipped": "⊘", "failed": "✗"}[status]
            dur_str = self._format_duration(duration)
            lines.append(f"  {symbol:<6} {name:<38} {dur_str:>10}")

            if status == "completed":
                completed += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1

        lines.append("")
        lines.append(f"Legend: ✓ = completed, ⊘ = skipped, ✗ = failed")
        lines.append(f"Completed: {completed} | Skipped: {skipped} | Failed: {failed}")
        lines.append("=" * 58)

        # Print and save
        summary = "\n".join(lines)
        print(summary)
        self.summary_file.write_text(summary + "\n")
        with open(self.log_file, 'a') as f:
            f.write(summary + "\n")

    def run(self, name, script, *args, check=True):
        """Run a pipeline step."""
        self._log(f"START | {name}")
        start = time.time()

        # Build command
        script_path = self.scripts_dir / script
        cmd = [sys.executable, str(script_path)] + [str(a) for a in args]

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.sim_dir),
                capture_output=False,  # Let output flow to terminal
                check=check
            )
            duration = time.time() - start
            self._log(f"DONE  | {name} ({int(duration)}s)")
            self.steps.append((name, "completed", duration))
            self._clear_swap()
            return True

        except subprocess.CalledProcessError as e:
            duration = time.time() - start
            self._log(f"FAIL  | {name} (exit code: {e.returncode})")
            self.steps.append((name, "failed", duration))
            self.failed = True
            if check:
                raise
            return False

    def skip(self, name, reason=None):
        """Mark a step as skipped."""
        msg = f"{name} ({reason})" if reason else name
        self._log(f"SKIP  | {msg}")
        self.steps.append((msg, "skipped", 0))

    def run_if(self, condition, name, script, *args):
        """Run step only if condition is True, otherwise skip."""
        if condition:
            return self.run(name, script, *args)
        else:
            self.skip(name, "condition not met")
            return True

    def skip_if(self, condition, name, script, *args, reason="exists"):
        """Skip step if condition is True, otherwise run."""
        if condition:
            self.skip(name, reason)
            return True
        else:
            return self.run(name, script, *args)

    def get_domains(self):
        """Get list of enabled domain paths."""
        return [d['path'] for d in get_enabled_domains(self.cfg)]

    def archive_exists(self, domain_path):
        """Check if domain has existing archive outputs."""
        archive_dir = Path(domain_path) / "sim_archive" / "outputs"
        if not archive_dir.exists():
            return False
        nc_files = list(archive_dir.glob("*.nc"))
        return len(nc_files) > 0


def main():
    """Run the full pipeline."""
    with Pipeline() as p:
        # Pipeline skip settings
        pipeline_cfg = p.cfg.get('pipeline', {})

        # Climate data
        if pipeline_cfg.get('skip_fetch_ifs', False):
            p.skip("Fetch IFS forecast", "disabled in config")
        else:
            p.run("Fetch IFS forecast", "fetch_ifs_forecast.py")

        if pipeline_cfg.get('skip_download_era5', False):
            p.skip("Download ERA5", "disabled in config")
        else:
            # download_era5.py needs a domain path with config.yml
            first_domain = p.get_domains()[0] if p.get_domains() else None
            if first_domain:
                p.run("Download ERA5", "download_era5.py", first_domain)
            else:
                p.skip("Download ERA5", "no domains configured")

        # Process each domain
        for domain in p.get_domains():
            domain_name = Path(domain).name

            # Init and archive (skip if exists)
            p.skip_if(
                p.archive_exists(domain),
                f"Init {domain_name} domain",
                "init_domain.py", domain,
                reason="sim_archive exists"
            )
            p.skip_if(
                p.archive_exists(domain),
                f"Run {domain_name} archive simulation",
                "run_archive_sim.py", domain,
                reason="sim_archive exists"
            )

            # Forecast simulation
            p.run(f"Run {domain_name} forecast simulation", "run_forecast_sim.py", domain)
            p.run(f"Merge {domain_name} FSM outputs", "merge_fsm_outputs.py", domain)
            p.run(f"Grid {domain_name} to NetCDF", "grid_fsm_to_netcdf.py", domain)

        # Post-processing
        p.run("Merge and reproject rasters", "merge_reproject.py", "./", "domains/D2000")
        p.run("Compute basin statistics", "compute_basin_stats.py")
        p.run("Compute zonal statistics", "zonal_stats.py")

        # Upload (if enabled)
        if p.cfg['upload'].get('enabled', False):
            p.run("Upload to S3", "upload_to_s3.py")
        else:
            p.skip("Upload to S3", "disabled in config")


if __name__ == "__main__":
    main()
