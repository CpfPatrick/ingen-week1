# Natural-photograph payload measurements

Six openly licensed photographs and photographic textures were measured to
supplement the earlier random-RGB experiment. These are actual image files, not
generated images, customer footage or captures from an Aido Rover. The experiment
measures encoded byte lengths; it does not measure network throughput or latency.

## Sources and reproducibility

[The source manifest](natural-payload-sources.json) pins the scikit-image data
repository to commit `e8a42ba85aaf5fd9322ef9ca51bc21063b22fcae` (v0.25.2), records
the download URL, source SHA-256, dimensions and byte count, and quotes that
version's image-specific license declaration. The photographs are:

| Source | Scene | Native dimensions | License / attribution in upstream source |
|---|---|---:|---|
| coffee | Cup, porcelain and wooden tabletop | 600 × 400 | CC0; Rachel Michetti, courtesy Pikolo Espresso Bar |
| chelsea | Cat, fur and fabric | 451 × 300 | CC0; Stefan van der Walt |
| rocket | Outdoor Falcon 9 DSCOVR launch | 640 × 427 | Public domain; SpaceX |
| brick | Photographic brick wall texture | 512 × 512 | CC0; CC0Textures Bricks25, processed by scikit-image |
| grass | Photographic grass texture | 512 × 512 | CC0; linolafett, processed by scikit-image |
| gravel | Photographic gravel texture | 512 × 512 | CC0; CC0Textures Gravel04, processed by scikit-image |

Brick, grass and gravel are cropped/processed grayscale photographs in the source
collection. The source collection's per-image declarations are the license
evidence; the repository's software license is not substituted for image rights.
The photos themselves need not be committed: the script can download the pinned
files and refuses bytes that do not match the manifest.

From the repository root, with the locked Python 3.11/Pillow environment:

```sh
uv sync --locked
uv run python scripts/measure_natural_payload.py \
  --images-dir /tmp/ingen-natural-photos --download \
  --output /tmp/natural-payload-measurements.json
cmp docs/natural-payload-measurements.json /tmp/natural-payload-measurements.json
```

An existing verified image directory can be used without `--download`. Optional
`--encoded-dir /tmp/ingen-natural-encoded` saves the 54 measured JPEGs for inspection.
Byte-for-byte replay was verified after downloading a fresh second copy. The
recorded codec environment is Pillow 12.3.0, libjpeg API 6.2, libjpeg-turbo 3.1.4.1.
Other codec builds may change exact JPEG bytes; report that difference instead of
silently replacing the recorded result.

## Protocol

Each image is converted to RGB, then encoded at its native dimensions, at
640 × 640 and at 1280 × 720. Fixed-size transforms use a centered crop-to-fit with
LANCZOS resampling. JPEG qualities are 60, 80 and 95; chroma subsampling is 4:2:0,
optimization and progressive encoding are disabled, and EXIF is stripped. There
is no random transform and therefore no random seed. Six images × three size
choices × three quality choices produces **54 encoded measurements**, plus six
original-file measurements.

All fixed-size transformations upscale at least one source dimension. The scale
factor and `upscaled` flag are recorded for each measurement. Upsampling cannot
recover native camera detail and may compress differently from a genuine 720p
camera image. The native-size results preserve a separate comparison group.

## Measured results

The complete reproducible output is
[natural-payload-measurements.json](natural-payload-measurements.json).
All sizes below are bytes. Each row contains six source images; p50 is the sample
median and p95 uses nearest rank. **With only six images, p95 equals the maximum**;
this is not an estimate of a field camera's tail distribution.

| Dimensions | JPEG quality | Minimum | p50 | p95 / maximum |
|---|---:|---:|---:|---:|
| Native, varied | 60 | 15,777 | 26,355.5 | 65,658 |
| Native, varied | 80 | 23,693 | 40,316.5 | 102,656 |
| Native, varied | 95 | 50,163 | 89,363.5 | 181,717 |
| 640 × 640 | 60 | 20,080 | 37,728 | 89,544 |
| 640 × 640 | 80 | 30,387 | 56,011.5 | 128,324 |
| 640 × 640 | 95 | 73,691 | 118,476.5 | 246,250 |
| 1280 × 720 | 60 | 43,914 | 72,966 | 139,443 |
| 1280 × 720 | 80 | 63,333 | 107,688.5 | 191,027 |
| 1280 × 720 | 95 | 137,361 | 229,391 | 347,293 |

For the 1280 × 720 quality-95 group, the six JPEG sizes are coffee 265,787;
chelsea 192,995; rocket 176,697; brick 137,361; grass 347,293; gravel 285,603.
The decoded 1280 × 720 RGB buffer is **2,764,800 bytes** independently of JPEG
quality. Original PNG/JPEG container sizes are recorded separately and are not
mixed into the JPEG-encoding sample statistics.

## What this establishes for frame transport

The checked-in Kafka configuration sets `message.max.bytes=1,048,588`. The Kafka
producer's default `max.request.size` is 1,048,576 bytes; this is the baseline
comparison in the measurement JSON, **not the effective Week 2 producer limit**.
The implemented `EventPublisher` explicitly sets `max.request.size=65,536` bytes
(64 KiB), because it carries Avro identity/metadata/reference events and never
image bytes. A proposed direct-image producer would require its own limit review;
the measured JPEG sizes must not be compared with the default as if the active
gateway were configured to transmit them. These limits apply at different layers:
neither is a guaranteed allowance for application payload alone. The
key, Avro framing, headers, record batch/request overhead and compression settings
also matter; consumer fetch limits need independent consideration.

Every natural-image JPEG in this experiment is below both numeric limits before
overhead. The largest is **347,293 bytes**, leaving **701,295 bytes** to the broker
limit and **701,283 bytes** to the producer limit before overhead. It would be
incorrect to claim this experiment proves that ordinary JPEGs cannot fit in Kafka.
Uncompressed 720p RGB exceeds the broker limit by 1,716,212 bytes, and the separate
[random-RGB stress fixture](PAYLOAD_EXPERIMENT.md) at quality 95 is 1,081,237 bytes,
32,649 bytes above the broker limit before overhead. That fixture remains a
synthetic stress case, not a natural-image distribution.

The natural measurements nevertheless quantify the broker traffic avoided by
claim-check. In an **illustrative assumption** of 10 robots at 5 fps (50 frames/s),
the mean 720p quality-80 size of 116,363 bytes implies **5,818,150 image bytes/s**,
and the largest measured 720p quality-95 frame implies **17,364,650 bytes/s** if
every frame had that size. Uncompressed RGB would imply 138,240,000 bytes/s. These
are arithmetic single-copy rates, excluding protocol overhead and replication;
they are not measured throughput or demonstrated capacity.

The decision is to store the image once in MinIO and put identity, metadata,
checksum and object key in the Kafka event. This avoids carrying each image
through every Kafka replica/consumer path and separates object lifetime from
event retention. It adds object-fetch latency, a MinIO dependency and orphan
cleanup/lifetime coordination. A reference event's actual encoded size must be
measured from its implemented Avro contract; this experiment does **not** invent a
measured metadata size. HTTP upload limits are a separate gateway policy, not
derived from this six-image maximum.

The remaining validation need is native-resolution, consented or open-licensed
outdoor camera footage with darkness, clutter, motion blur and varying encoder
settings, followed by actual broker/object-store load tests. These six handpicked
photos establish a reproducible natural-image comparison, not product capacity,
a retention policy, or a safe production maximum.
