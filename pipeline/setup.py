"""Packages the pipeline modules so Dataflow workers can import them
(batch_backfill imports ParseEvent from streaming_pipeline). Passed with --setup_file."""

from setuptools import setup

setup(
    name="incident_pipeline",
    version="0.1.0",
    py_modules=["streaming_pipeline", "batch_backfill"],
    install_requires=[],
)
