"""Helpers for the registry end-to-end tests.

There is one module: ``events``, which runs a step exactly as the pipeline runtime
does and collects what it yields. Nothing here replaces or fakes any part of the
system - the tests talk to the real publishers and the real MongoDB.
"""
