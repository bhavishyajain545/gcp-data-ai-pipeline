"""Tiny demo-video recorder: Playwright screen video + neural TTS narration.

How it works
------------
* One Chromium context records a continuous 1920x1080 video.
* Each `scene()` generates narration audio (edge-tts, free Microsoft neural
  voices), notes the scene's start offset, runs the on-screen actions, and holds
  the frame until the narration has finished.
* `finish()` muxes the narration clips onto the video at their offsets with
  ffmpeg and writes `demo.mp4`, `captions.srt` and `transcript.md`.

Nothing here is project specific; each project keeps its own `scenes.py`.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

import edge_tts
from playwright.async_api import BrowserContext, Page, async_playwright

FFMPEG = shutil.which("ffmpeg") or r"C:\Users\admin\AppData\Local\Programs\ffmpeg\ffmpeg-9.0.2-essentials_build\bin\ffmpeg.exe"
FFPROBE = str(Path(FFMPEG).with_name(Path(FFMPEG).name.replace("ffmpeg", "ffprobe")))


@dataclass
class SceneRecord:
    title: str
    narration: str
    audio: Path
    start: float
    audio_len: float
    end: float = 0.0


@dataclass
class Demo:
    out_dir: Path
    voice: str = "en-US-Chirp3-HD-Charon"  # Google Cloud TTS voice; use an edge-tts voice name with tts="edge"
    tts: str = "google"                     # "google" (Cloud Text-to-Speech, ADC auth) or "edge" (free Microsoft voices)
    tts_project: str = "bj-gemini-agent"    # quota/billing project for the Text-to-Speech API
    rate: str = "+0%"
    size: tuple[int, int] = (1920, 1080)
    profile_dir: Path | None = None  # persistent Chrome profile (for console pages that need login)
    headless: bool = True            # recording works headless; set False to watch it run
    cdp_url: str | None = None       # attach to a running, signed-in Chrome (--remote-debugging-port) and capture via screencast
    max_silence: float = 3.0         # after narration ends, keep at most this much of a still-running action (CDP mode)
    tail: float = 2.0                # ...but always keep the last seconds of the scene so the result is seen
    scenes: list[SceneRecord] = field(default_factory=list)
    _pw = None
    _ctx: BrowserContext | None = None
    _t0: float = 0.0
    page: Page | None = None
    _frames: list[tuple[float, Path]] = field(default_factory=list)
    _cdp = None
    _frames_dir: Path | None = None

    # ------------------------------------------------------------------ setup
    async def start(self) -> Page:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        (self.out_dir / "tts").mkdir(exist_ok=True)
        self._video_dir = self.out_dir / "_video"
        shutil.rmtree(self._video_dir, ignore_errors=True)
        self._pw = await async_playwright().start()
        w, h = self.size
        if self.cdp_url:
            return await self._start_cdp(w, h)
        kwargs = dict(
            channel="chrome",
            headless=self.headless,
            viewport={"width": w, "height": h},
            record_video_dir=str(self._video_dir),
            record_video_size={"width": w, "height": h},
            args=[f"--window-size={w},{h + 90}", "--hide-scrollbars", "--force-device-scale-factor=1"],
            ignore_default_args=["--enable-automation"],
        )
        if self.profile_dir:
            self._ctx = await self._pw.chromium.launch_persistent_context(str(self.profile_dir), **kwargs)
            self.page = self._ctx.pages[0] if self._ctx.pages else await self._ctx.new_page()
        else:
            browser = await self._pw.chromium.launch(channel="chrome", headless=self.headless, args=kwargs["args"])
            self._ctx = await browser.new_context(
                viewport=kwargs["viewport"],
                record_video_dir=kwargs["record_video_dir"],
                record_video_size=kwargs["record_video_size"],
            )
            self.page = await self._ctx.new_page()
        self._t0 = time.monotonic()
        return self.page

    async def _start_cdp(self, w: int, h: int) -> Page:
        """Open a fresh window in the user's running Chrome and record it with Page.screencast."""
        browser = await self._pw.chromium.connect_over_cdp(self.cdp_url)
        self._ctx = browser.contexts[0]
        self.page = await self._ctx.new_page()
        self._cdp = await self._ctx.new_cdp_session(self.page)
        # Put the tab in its own window sized for the recording.
        wid = (await self._cdp.send("Browser.getWindowForTarget"))["windowId"]
        await self._cdp.send("Browser.setWindowBounds", {"windowId": wid, "bounds": {"windowState": "normal", "left": 0, "top": 0, "width": w + 16, "height": h + 130}})
        await self.page.set_viewport_size({"width": w, "height": h})
        await self.page.bring_to_front()
        self._frames_dir = self._video_dir / "frames"
        self._frames_dir.mkdir(parents=True, exist_ok=True)
        self._frames = []
        self._t0 = time.monotonic()
        await self._start_screencast(self._cdp)
        return self.page

    async def _start_screencast(self, cdp) -> None:
        w, h = self.size

        def on_frame(params, _cdp=cdp):
            self._frames.append((time.monotonic() - self._t0, params["data"]))
            asyncio.ensure_future(_cdp.send("Page.screencastFrameAck", {"sessionId": params["sessionId"]}))

        cdp.on("Page.screencastFrame", on_frame)
        await cdp.send("Page.startScreencast", {"format": "jpeg", "quality": 85, "maxWidth": w, "maxHeight": h, "everyNthFrame": 1})

    async def new_tab(self, url: str) -> Page:
        """Open another tab in the recorded browser (e.g. to pre-load a slow console page)."""
        assert self._ctx
        w, h = self.size
        tab = await self._ctx.new_page()
        await tab.set_viewport_size({"width": w, "height": h})
        await tab.goto(url, wait_until="commit", timeout=120000)
        return tab

    async def capture(self, page: Page) -> None:
        """Switch the screencast to another tab (CDP mode); the timeline keeps running."""
        if not self.cdp_url or page is self.page:
            self.page = page
            return
        if self._cdp:
            try:
                await self._cdp.send("Page.stopScreencast")
            except Exception:
                pass
        self.page = page
        await page.bring_to_front()
        self._cdp = await self._ctx.new_cdp_session(page)
        await self._start_screencast(self._cdp)
        await asyncio.sleep(0.4)

    # ------------------------------------------------------------------ narration
    async def prepare(self, *texts: str) -> None:
        """Synthesize narration up front so no network wait lands inside the recording."""
        for t in texts:
            await self._tts(t)

    async def _tts(self, text: str) -> tuple[Path, float]:
        key = hashlib.sha1(f"{self.tts}|{self.voice}|{self.rate}|{text}".encode()).hexdigest()[:12]
        mp3 = self.out_dir / "tts" / f"{key}.mp3"
        mp3.parent.mkdir(parents=True, exist_ok=True)
        if not mp3.exists():
            if self.tts == "google":
                await asyncio.to_thread(google_tts, text, self.voice, mp3, self.tts_project)
            else:
                await edge_tts.Communicate(text, self.voice, rate=self.rate).save(str(mp3))
        return mp3, media_duration(mp3)

    async def scene(self, title: str, narration: str, action=None, hold: float = 0.8, pre: float = 0.3) -> None:
        """Narrate `narration` while running `action()` (a coroutine); hold until audio ends."""
        audio, audio_len = await self._tts(narration)
        await asyncio.sleep(pre)
        start = time.monotonic() - self._t0
        rec = SceneRecord(title, narration, audio, start, audio_len)
        self.scenes.append(rec)
        print(f"[{start:7.2f}s] {title}  (audio {audio_len:.1f}s)")
        t = time.monotonic()
        if action is not None:
            try:
                await action()
            except Exception as exc:  # keep recording; a slow page must not abort a 5-minute take
                print(f"  ! scene '{title}' action failed: {type(exc).__name__}: {str(exc)[:160]}")
        elapsed = time.monotonic() - t
        remaining = audio_len - elapsed + hold
        if remaining > 0:
            await asyncio.sleep(remaining)
        rec.end = time.monotonic() - self._t0

    # ------------------------------------------------------------------ helpers
    async def terminal(self, title: str, steps: list[tuple[str, str]], type_delay: int = 18, pause: float = 0.6) -> None:
        """Render a terminal-looking page and 'type' real commands with their captured output."""
        assert self.page
        # Leave any CSP/Trusted-Types page (e.g. the Cloud console) before writing our own document.
        await self.page.goto("about:blank")
        await self.page.set_content(_terminal_html(title))
        for cmd, output in steps:
            await self.page.evaluate("(c) => window.__prompt(c)", "")
            for ch in cmd:
                await self.page.evaluate("(c) => window.__type(c)", ch)
                await asyncio.sleep(type_delay / 1000)
            await asyncio.sleep(pause)
            await self.page.evaluate("(o) => window.__output(o)", output)
            await asyncio.sleep(pause)

    async def type_into(self, selector: str, text: str, delay: int = 35) -> None:
        assert self.page
        await self.page.click(selector)
        await self.page.keyboard.type(text, delay=delay)

    async def highlight(self, selector: str, seconds: float = 1.5) -> None:
        """Draw a red outline around an element for a moment."""
        assert self.page
        await self.page.evaluate(
            """([sel, ms]) => { const el = document.querySelector(sel); if (!el) return;
                 const prev = el.style.outline; el.style.outline = '4px solid #e5484d'; el.style.outlineOffset = '4px';
                 el.scrollIntoView({block: 'center'}); setTimeout(() => { el.style.outline = prev; }, ms); }""",
            [selector, int(seconds * 1000)],
        )
        await asyncio.sleep(seconds)

    # ------------------------------------------------------------------ finish
    async def finish(self, name: str = "demo") -> Path:
        assert self._ctx and self.page
        if self.cdp_url:
            raw = await self._finish_cdp()
        else:
            video = self.page.video
            await self._ctx.close()
            raw = Path(await video.path()) if video else next(self._video_dir.glob("*.webm"))
        if self._pw:
            await self._pw.stop()

        out = self.out_dir / f"{name}.mp4"
        cmd = [FFMPEG, "-y", "-i", str(raw)]
        filters, labels = [], []
        for i, s in enumerate(self.scenes):
            cmd += ["-i", str(s.audio)]
            filters.append(f"[{i + 1}:a]adelay={int(s.start * 1000)}|{int(s.start * 1000)}[a{i}]")
            labels.append(f"[a{i}]")
        filters.append(f"{''.join(labels)}amix=inputs={len(labels)}:normalize=0:dropout_transition=0[aout]")
        cmd += [
            "-filter_complex", ";".join(filters),
            "-map", "0:v", "-map", "[aout]",
            "-c:v", "libx264", "-preset", "medium", "-crf", "20", "-pix_fmt", "yuv420p", "-r", "30",
            "-c:a", "aac", "-b:a", "160k", "-shortest", str(out),
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        self._write_captions(name)
        self._write_transcript(name)
        shutil.rmtree(self._video_dir, ignore_errors=True)
        print(f"wrote {out} ({media_duration(out):.1f}s)")
        return out

    async def _finish_cdp(self) -> Path:
        """Stop the screencast, dump frames and turn them into a constant-30fps webm via ffmpeg concat."""
        import base64

        await asyncio.sleep(0.5)
        await self._cdp.send("Page.stopScreencast")
        end = time.monotonic() - self._t0
        await self.page.close()
        assert self._frames_dir
        listing = []
        w, h = self.size
        # Anchor the timeline at t=0 so narration offsets stay aligned even if the
        # first screencast frame arrives late (blank pages emit no frames).
        if self._frames and self._frames[0][0] > 0.05:
            self._frames.insert(0, (0.0, self._frames[0][1]))
        end = self._cut_dead_air(end)
        for i, (t, data) in enumerate(self._frames):
            p = self._frames_dir / f"f{i:06d}.jpg"
            p.write_bytes(base64.b64decode(data))
            nxt = self._frames[i + 1][0] if i + 1 < len(self._frames) else end
            listing.append(f"file '{p.as_posix()}'\nduration {max(nxt - t, 0.001):.4f}")
        if listing:
            listing.append(f"file '{(self._frames_dir / f'f{len(self._frames) - 1:06d}.jpg').as_posix()}'")
        concat = self._video_dir / "frames.txt"
        concat.write_text("\n".join(listing), encoding="utf-8")
        raw = self._video_dir / "raw.mp4"
        subprocess.run(
            [FFMPEG, "-y", "-f", "concat", "-safe", "0", "-i", str(concat),
             "-vf", f"scale={w}:{h}:force_original_aspect_ratio=decrease,pad={w}:{h}:(ow-iw)/2:(oh-ih)/2,fps=30",
             "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p", str(raw)],
            check=True, capture_output=True,
        )
        print(f"screencast: {len(self._frames)} frames over {end:.1f}s")
        return raw

    def _cut_dead_air(self, end: float) -> float:
        """Drop footage where an action keeps running long after its narration ended.

        For each scene keep [start, start+audio+hold+max_silence] and [end-tail, end];
        everything between is a jump cut (page loading, model thinking). Frame
        timestamps and scene offsets are remapped onto the shortened timeline.
        """
        cuts: list[tuple[float, float]] = []
        if self.scenes and self.scenes[0].start > 1.0:  # setup time before the first scene (tab pre-loads etc.)
            cuts.append((0.0, self.scenes[0].start - 0.3))
        for s in self.scenes:
            keep_until = s.start + s.audio_len + 0.8 + self.max_silence
            resume_at = s.end - self.tail
            if resume_at - keep_until > 0.5:
                cuts.append((keep_until, resume_at))
        if not cuts:
            return end

        def remap(t: float) -> float | None:
            removed = 0.0
            for a, b in cuts:
                if a <= t < b:
                    return None
                if t >= b:
                    removed += b - a
            return t - removed

        kept = []
        for t, data in self._frames:
            nt = remap(t)
            if nt is not None:
                kept.append((nt, data))
            else:
                # keep the last frame before a cut so the jump lands on the final state
                pass
        self._frames = kept
        for s in self.scenes:
            s.start = remap(s.start) or 0.0
            s.end = remap(s.end - 0.001) or s.start
        total_cut = sum(b - a for a, b in cuts)
        print(f"dead air removed: {total_cut:.1f}s across {len(cuts)} scenes")
        return end - total_cut

    def _write_captions(self, name: str) -> None:
        cues, n = [], 1
        for s in self.scenes:
            sentences = [x.strip() for x in re.split(r"(?<=[.!?])\s+", s.narration) if x.strip()]
            total = sum(len(x) for x in sentences) or 1
            t = s.start
            for sent in sentences:
                dur = s.audio_len * len(sent) / total
                cues.append(f"{n}\n{_srt(t)} --> {_srt(t + dur)}\n{sent}\n")
                t += dur
                n += 1
        (self.out_dir / "captions.srt").write_text("\n".join(cues), encoding="utf-8")

    def _write_transcript(self, name: str) -> None:
        lines = [f"# Demo transcript: {name}", ""]
        for s in self.scenes:
            lines += [f"## {_mmss(s.start)}  {s.title}", "", s.narration, ""]
        (self.out_dir / "transcript.md").write_text("\n".join(lines), encoding="utf-8")
        (self.out_dir / "scenes.json").write_text(
            json.dumps([{"title": s.title, "start": round(s.start, 2), "end": round(s.end, 2)} for s in self.scenes], indent=2)
        )


# ---------------------------------------------------------------------- utils
_token_cache: dict[str, str] = {}


def google_tts(text: str, voice: str, out: Path, project: str) -> None:
    """Cloud Text-to-Speech via REST, authenticated with the gcloud user login (no keys)."""
    import base64
    import urllib.request

    if "token" not in _token_cache:
        gcloud = shutil.which("gcloud") or shutil.which("gcloud.cmd") or "gcloud"
        _token_cache["token"] = subprocess.run(
            [gcloud, "auth", "print-access-token"], capture_output=True, text=True, check=True, shell=gcloud.endswith(".cmd")
        ).stdout.strip()
    token = _token_cache["token"]
    body = json.dumps({
        "input": {"text": text},
        "voice": {"languageCode": voice[:5], "name": voice},
        "audioConfig": {"audioEncoding": "MP3", "speakingRate": 1.0},
    }).encode()
    req = urllib.request.Request(
        "https://texttospeech.googleapis.com/v1/text:synthesize", data=body, method="POST",
        headers={"Authorization": f"Bearer {token}", "x-goog-user-project": project, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        audio = json.load(resp)["audioContent"]
    out.write_bytes(base64.b64decode(audio))


def media_duration(path: Path) -> float:
    r = subprocess.run(
        [FFPROBE, "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)],
        capture_output=True, text=True, check=True,
    )
    return float(r.stdout.strip())


def _srt(t: float) -> str:
    ms = int(round(t * 1000))
    h, ms = divmod(ms, 3_600_000)
    m, ms = divmod(ms, 60_000)
    s, ms = divmod(ms, 1000)
    return f"{h:02}:{m:02}:{s:02},{ms:03}"


def _mmss(t: float) -> str:
    return f"{int(t // 60):02}:{int(t % 60):02}"


def _terminal_html(title: str) -> str:
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>{html.escape(title)}</title>
<style>
 html,body{{margin:0;height:100%;background:#0b0f14;color:#d7dde5;font:22px/1.45 Consolas,'Cascadia Mono',monospace}}
 .bar{{height:44px;background:#151b23;display:flex;align-items:center;padding:0 18px;color:#8b98a5;font-size:18px;border-bottom:1px solid #222a33}}
 .dot{{width:12px;height:12px;border-radius:50%;margin-right:8px;display:inline-block}}
 #t{{padding:22px 28px;white-space:pre-wrap;word-break:break-all;overflow:hidden;height:calc(100% - 90px)}}
 .p{{color:#7ee787}} .c{{color:#e6edf3}} .o{{color:#9aa7b4}}
 .cur::after{{content:'▍';color:#d7dde5;animation:b 1s steps(1) infinite}} @keyframes b{{50%{{opacity:0}}}}
</style></head><body>
<div class="bar"><span class="dot" style="background:#ff5f57"></span><span class="dot" style="background:#febc2e"></span><span class="dot" style="background:#28c840"></span>&nbsp;&nbsp;{html.escape(title)}</div>
<div id="t"></div>
<script>
 const t=document.getElementById('t'); let line=null;
 window.__prompt=()=>{{ line=document.createElement('div'); line.innerHTML='<span class="p">$ </span><span class="c cur"></span>'; t.appendChild(line); scroll(); }};
 window.__type=(ch)=>{{ line.querySelector('.c').textContent+=ch; }};
 window.__output=(o)=>{{ line.querySelector('.c').classList.remove('cur'); const d=document.createElement('div'); d.className='o'; d.textContent=o; t.appendChild(d); scroll(); }};
 function scroll(){{ while(t.scrollHeight>t.clientHeight && t.children.length>1) t.removeChild(t.firstChild); }}
</script></body></html>"""
