# Synthetic payload-size experiment

[measure_payload.py](../scripts/measure_payload.py) encodes one deterministic random-RGB
fixture. It measures file sizes, not robot performance or Kafka throughput.

Reproduce from the repository root:

```sh
uv sync --locked
uv run python scripts/measure_payload.py
```

The checked-in [recorded output](synthetic-payload-measurements.json) uses Pillow 12.3.0,
seed 20261102, 1280 by 720 RGB pixels, and JPEG optimize=false/progressive=false.
The uncompressed RGB buffer is 2,764,800 bytes.

| JPEG quality | Encoded bytes | Margin to 1,048,588 bytes |
|---|---:|---:|
| 75 | 554,395 | 494,193 |
| 90 | 826,496 | 222,092 |
| 95 | 1,081,237 | -32,649 |

The configured Kafka record-batch limit is not an available application-payload
allowance: framing, keys, headers and batching reduce the usable margin. Producer and
consumer limits must be checked separately. One artificial fixture does not establish
a natural-image size distribution.

The JSON also multiplies each size by an illustrative 50 frames per second. These are
arithmetic rates, not measured transfer rates, latency results or demonstrated capacity.
The skeleton does not implement frame ingestion or a claim-check processing pipeline.
