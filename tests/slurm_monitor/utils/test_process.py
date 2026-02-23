import pytest
import subprocess
import os


@pytest.mark.parametrize(
    "lines,expected_active_jobs",
    [
        [
            [
                "PID      JOBID    STEPID   LOCALID GLOBALID",
                "No job steps exist on this node.",
            ],
            0,
        ],
        [
            [
                "wrong-format-header",
                "-1      10   0 0 0",
                "0      -1   0 0 0",
                "wrong-format-row",
            ],
            0,
        ],
        [
            [
                "PID      JOBID    STEPID   LOCALID GLOBALID",
                "1   10   0        0       0",
                f"{os.getpid()}   20   0        -       -",
            ],
            2,
        ],
    ],
)
def test_job_monitor_get_active_jobs(lines, expected_active_jobs, monkeypatch, mocker):
    mock_slurm = mocker.patch("slurm_monitor.utils.slurm.Slurm")
    mock_slurm.ensure.return_value = "scontrol"

    original_subprocess_fn = subprocess.run
    output = "\n".join(lines)

    def subprocess_mock(cmd, **kwargs):
        if cmd.startswith("pidof docker run"):
            return original_subprocess_fn("pidof docker--run", **kwargs)

        return original_subprocess_fn(f"echo '{output}'", **kwargs)

    monkeypatch.setattr(subprocess, "run", subprocess_mock)

    from slurm_monitor.utils.process import JobMonitor

    jobs = JobMonitor.get_active_jobs()
    assert len(jobs.jobs) == expected_active_jobs
