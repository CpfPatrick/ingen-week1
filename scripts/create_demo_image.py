"""Create a small synthetic JPEG for the documented gateway curl walkthrough."""

import sys
from pathlib import Path

from PIL import Image, ImageDraw


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python scripts/create_demo_image.py OUTPUT.jpg")
    target = Path(sys.argv[1])
    target.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", (640, 480), "#102034")
    drawing = ImageDraw.Draw(image)
    drawing.rectangle((120, 100, 520, 380), fill="#45bfb0")
    drawing.ellipse((240, 170, 400, 330), fill="#f0cf75")
    image.save(target, format="JPEG", quality=80, optimize=False, progressive=False)
    print(target)


if __name__ == "__main__":
    main()
