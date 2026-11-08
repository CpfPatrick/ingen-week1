"""Deterministic synthetic JPEG demonstration; not a natural-image workload benchmark.

Run with Pillow 12.3.0: python scripts/measure_payload.py
"""
import io
import json
import random

import PIL
from PIL import Image


def main() -> None:
    width, height = 1280, 720
    rng = random.Random(20261102)
    # Locally generated random RGB only: no person, robot, or private source image.
    pixels = bytes(rng.randrange(256) for _ in range(width * height * 3))
    frame = Image.frombytes("RGB", (width, height), pixels)
    result: list[dict[str, object]] = []
    for quality in (75, 90, 95):
        output = io.BytesIO()
        frame.save(output, format="JPEG", quality=quality, optimize=False, progressive=False)
        size = len(output.getvalue())
        result.append({
            "quality": quality,
            "jpeg_bytes": size,
            "raw_rgb_bytes": len(pixels),
            "frames_per_second": 50,
            "ten_robots_at_5fps_bytes_per_second": 50 * size,
            "configured_message_max_bytes": 1048588,
            "remaining_before_record_overhead_bytes": 1048588 - size,
        })
    print(json.dumps({"pillow": PIL.__version__, "seed": 20261102,
                      "width": width, "height": height, "synthetic_only": True,
                      "measurements": result}, indent=2))


if __name__ == "__main__":
    main()
