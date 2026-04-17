from __future__ import annotations

import os
from pathlib import Path


def _find_winutils_candidates() -> list[Path]:
    candidates: list[Path] = []

    hadoop_home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    if hadoop_home:
        candidates.append(Path(hadoop_home) / "bin" / "winutils.exe")

    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        candidates.append(Path(spark_home) / "bin" / "winutils.exe")

    repo_candidate = Path(__file__).resolve().parents[2] / "tools" / "winutils" / "bin" / "winutils.exe"
    candidates.append(repo_candidate)
    return candidates


def validate_local_spark_runtime() -> None:
    if os.name != "nt":
        return

    java_home = os.environ.get("JAVA_HOME", "")
    java_exe = Path(java_home) / "bin" / "java.exe"
    if not java_home or not java_exe.exists():
        raise RuntimeError(
            "Invalid JAVA_HOME for local Spark on Windows. "
            "Please set JAVA_HOME to a valid JDK root, for example: "
            "C:/Program Files/Eclipse Adoptium/jdk-17.0.16.8-hotspot"
        )

    if any(path.exists() for path in _find_winutils_candidates()):
        return

    raise RuntimeError(
        "winutils.exe was not found. Local Spark on Windows needs winutils. "
        "Fix options: "
        "1) run the containerized pipeline: powershell -ExecutionPolicy Bypass -File scripts/run_batch_pipeline.ps1; "
        "2) install winutils.exe and set HADOOP_HOME to its parent directory (which contains bin/winutils.exe)."
    )
