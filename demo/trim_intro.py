"""Cut dead time before the first scene from demo.mp4 and shift captions/transcript/scenes.

    python demo/trim_intro.py            # uses scenes.json to find the first scene
"""

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from recorder import FFMPEG, media_duration, _srt  # noqa: E402

scenes = json.loads((HERE / "scenes.json").read_text())
offset = max(scenes[0]["start"] - 0.3, 0.0)
if offset < 0.5:
    print("nothing to trim"); sys.exit(0)

src, tmp = HERE / "demo.mp4", HERE / "demo.trim.mp4"
subprocess.run([FFMPEG, "-y", "-loglevel", "error", "-ss", f"{offset:.3f}", "-i", str(src),
                "-c:v", "libx264", "-preset", "medium", "-crf", "20", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "160k", str(tmp)], check=True)
shutil.move(tmp, src)

def shift_srt(line: str) -> str:
    def f(m):
        h, mi, s, ms = map(int, m.groups())
        t = max(h * 3600 + mi * 60 + s + ms / 1000 - offset, 0.0)
        return _srt(t)
    return re.sub(r"(\d\d):(\d\d):(\d\d),(\d{3})", f, line)

srt = HERE / "captions.srt"
srt.write_text("\n".join(shift_srt(l) if "-->" in l else l for l in srt.read_text(encoding="utf-8").splitlines()) + "\n", encoding="utf-8")

for s in scenes:
    s["start"] = round(max(s["start"] - offset, 0), 2)
    s["end"] = round(max(s["end"] - offset, 0), 2)
(HERE / "scenes.json").write_text(json.dumps(scenes, indent=2))

tr = HERE / "transcript.md"
lines = tr.read_text(encoding="utf-8").splitlines()
i = 0
for n, line in enumerate(lines):
    m = re.match(r"^## (\d\d):(\d\d)  (.*)$", line)
    if m and i < len(scenes):
        t = scenes[i]["start"]
        lines[n] = f"## {int(t // 60):02}:{int(t % 60):02}  {m.group(3)}"
        i += 1
tr.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"trimmed {offset:.2f}s -> {media_duration(src):.1f}s")
