"""Reproducible synthetic data generators.

Everything is driven by a single integer seed (DATAFORGE_SEED). The same seed and scale
always produce byte-identical raw files, including the injected data-quality defects,
whose exact counts are written to a manifest so that tests can verify the pipeline
recovers them.

ALL DATA PRODUCED HERE IS SYNTHETIC. Names, emails and addresses are assembled from
word lists and never correspond to real people or companies.
"""
