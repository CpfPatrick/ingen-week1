"""Reproduce payload-size measurements from six pinned, openly licensed photographs.

This measures encoded byte lengths and arithmetic load scenarios, not robot or
Kafka throughput. No photos are generated and no random noise is added.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import statistics
import urllib.request
from pathlib import Path
from typing import Any, cast

import PIL
from PIL import Image, ImageOps, features

BROKER_LIMIT = 1_048_588
PRODUCER_LIMIT = 1_048_576
ACTIVE_METADATA_PRODUCER_LIMIT = 65_536
QUALITIES = (60, 80, 95)
TARGETS: tuple[tuple[str, tuple[int, int] | None], ...] = (
    ("native", None), ("640x640", (640, 640)), ("1280x720", (1280, 720)),
)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def summary(values: list[int]) -> dict[str, int | float]:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min_bytes": ordered[0],
        "p50_bytes": statistics.median(ordered),
        "p95_nearest_rank_bytes": ordered[math.ceil(0.95 * len(ordered)) - 1],
        "max_bytes": ordered[-1],
        "mean_bytes": round(statistics.mean(ordered), 3),
    }


def checked_source(source: dict[str, Any], images_dir: Path, download: bool) -> Path:
    name = str(source["file"])
    if Path(name).name != name:
        raise ValueError("Manifest image filename must not contain a directory")
    path = images_dir / name
    if not path.exists() and download:
        url = str(source["source_url"])
        if not url.startswith("https://raw.githubusercontent.com/scikit-image/scikit-image/"):
            raise ValueError("Only the pinned scikit-image sample source is downloadable")
        with urllib.request.urlopen(url, timeout=30) as response:
            data = response.read(10 * 1024 * 1024 + 1)
        if len(data) > 10 * 1024 * 1024:
            raise ValueError("Source exceeds the 10 MiB download limit")
        if sha256(data) != source["sha256"]:
            raise ValueError(f"Downloaded source checksum mismatch: {name}")
        images_dir.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    data = path.read_bytes()
    if len(data) != source["source_bytes"] or sha256(data) != source["sha256"]:
        raise ValueError(f"Source size/checksum mismatch: {name}")
    return path


def measure(manifest: dict[str, Any], images_dir: Path, download: bool,
            encoded_dir: Path | None) -> dict[str, Any]:
    measurements: list[dict[str, Any]] = []
    originals: list[dict[str, Any]] = []
    sources = cast(list[dict[str, Any]], manifest["sources"])
    for source in sources:
        path = checked_source(source, images_dir, download)
        with Image.open(path) as opened:
            opened.load()
            original_format = opened.format
            rgb = ImageOps.exif_transpose(opened).convert("RGB")
        if rgb.size != (source["source_width"], source["source_height"]):
            raise ValueError(f"Source dimensions mismatch: {path.name}")
        originals.append({
            "source_id": source["id"], "file": path.name,
            "format": original_format, "width": rgb.width, "height": rgb.height,
            "source_file_bytes": source["source_bytes"],
            "decoded_rgb_bytes": rgb.width * rgb.height * 3,
            "sha256": source["sha256"],
        })
        for target_name, dimensions in TARGETS:
            frame = rgb if dimensions is None else ImageOps.fit(
                rgb, dimensions, method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))
            scale = 1.0 if dimensions is None else max(
                frame.width / rgb.width, frame.height / rgb.height)
            for quality in QUALITIES:
                output = io.BytesIO()
                frame.save(output, format="JPEG", quality=quality, subsampling=2,
                           optimize=False, progressive=False, exif=b"")
                payload = output.getvalue()
                count = len(payload)
                encoded_name = f"{source['id']}-{target_name}-q{quality}.jpg"
                if encoded_dir is not None:
                    encoded_dir.mkdir(parents=True, exist_ok=True)
                    (encoded_dir / encoded_name).write_bytes(payload)
                measurements.append({
                    "source_id": source["id"], "transform": target_name,
                    "width": frame.width, "height": frame.height,
                    "scale_factor": round(scale, 9), "upscaled": scale > 1.0,
                    "jpeg_quality": quality, "jpeg_subsampling": "4:2:0",
                    "jpeg_bytes": count, "jpeg_sha256": sha256(payload),
                    "decoded_rgb_bytes": frame.width * frame.height * 3,
                    "margin_to_broker_limit_before_record_overhead_bytes": BROKER_LIMIT - count,
                    "margin_to_producer_limit_before_request_overhead_bytes": PRODUCER_LIMIT-count,
                    "illustrative_50_frames_per_second_bytes_per_second": 50 * count,
                    "encoded_filename": encoded_name,
                })
    groups: dict[str, Any] = {}
    for target_name, _ in TARGETS:
        subset = [row for row in measurements if row["transform"] == target_name]
        groups[target_name] = summary([int(row["jpeg_bytes"]) for row in subset])
        groups[target_name]["by_quality"] = {
            str(q): summary([int(row["jpeg_bytes"]) for row in subset if row["jpeg_quality"] == q])
            for q in QUALITIES
        }
    return {
        "experiment": "natural-photograph-payload-size-v1",
        "source_manifest_sha256": sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()),
        "source_commit": manifest["source_commit"],
        "pillow": PIL.__version__, "libjpeg": features.version_codec("jpg"),
        "libjpeg_turbo": features.version_feature("libjpeg_turbo"),
        "protocol": {
            "selection": manifest["sampling"],
            "transform": "RGB, native or center-crop-to-fit with LANCZOS; upscaling recorded",
            "randomness": "None; deterministic transforms require no seed",
            "jpeg": {"qualities": list(QUALITIES), "subsampling": "4:2:0",
                     "optimize": False, "progressive": False, "exif": "stripped"},
            "p95": "nearest rank; with six samples in a quality group this equals maximum",
            "broker_record_batch_limit_bytes": BROKER_LIMIT,
            "producer_default_max_request_size_bytes": PRODUCER_LIMIT,
            "active_gateway_metadata_only_max_request_size_bytes": ACTIVE_METADATA_PRODUCER_LIMIT,
            "producer_limit_scope": "The 1 MiB comparison is the Kafka default baseline. "
                "The implemented Week 2 producer uses 64 KiB for Avro metadata only, never images.",
            "load_scenario": "Assumption only: 10 robots at 5 fps = 50 frames/s; not measured capacity",
        },
        "originals": originals,
        "measurements": measurements,
        "summary_by_transform": groups,
        "measured_throughput_or_latency": False,
        "real_robot_camera_capture": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path,
                        default=Path("docs/natural-payload-sources.json"))
    parser.add_argument("--images-dir", type=Path, required=True)
    parser.add_argument("--download", action="store_true",
                        help="Download only missing pinned photos and verify their SHA-256")
    parser.add_argument("--encoded-dir", type=Path,
                        help="Optional directory for inspecting the 54 encoded JPEGs")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    manifest = cast(dict[str, Any], json.loads(args.manifest.read_text()))
    result = measure(manifest, args.images_dir, args.download, args.encoded_dir)
    content = json.dumps(result, indent=2) + "\n"
    if args.output is None:
        print(content, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content)


if __name__ == "__main__":
    main()
