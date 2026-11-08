# Assistant component

This directory reserves the Python assistant component in the development skeleton.
There is no assistant runtime, retrieval pipeline, model integration or public endpoint yet.

Future implementation should use authorized, read-only query interfaces and grounded
retrieval. It must not receive database credentials or make robot-control decisions.
The shared Python environment and dependency lock are at the repository root.
