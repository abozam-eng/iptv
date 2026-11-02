#!/usr/bin/env python3
"""Prüft die Streams in DomaTV_Russische_Sender.m3u8.

Die Playlist wird nur gelesen, nie verändert und nie automatisch gekürzt.
Ergebnis landet in status/status.json und status/BERICHT.md.
Nur die Python-Standardbibliothek.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import socket
import ssl
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PLAYLIST = ROOT / "DomaTV_Russische_Sender.m3u8"
DEFAULT_STATUS = ROOT / "status" / "status.json"
DEFAULT_REPORT = ROOT / "status" / "BERICHT.md"

TIMEOUT_SECONDS = 10
WORKERS = 16
MAX_BYTES = 128 * 1024
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 "
    "VLC/3.0.21 LibVLC/3.0.21"
)
PLAYLIST_NAME = "DomaTV_Russische_Sender.m3u8"

ATTR_RE = re.compile(r'([\w-]+)="([^"]*)"')
STREAM_INF_URI_RE = re.compile(r'URI="([^"]+)"')
PRINT_LOCK = threading.Lock()
SSL_CONTEXT = ssl.create_default_context()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_playlist(text: str) -> list[dict]:
    """Liest #EXTINF-Zeilen. Die Stream-Adresse bleibt nur intern zum Prüfen."""
    entries: list[dict] = []
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        line = lines[index].strip()
        if line.startswith("#EXTINF"):
            attrs = dict(ATTR_RE.findall(line))
            display = line.split(",", 1)[1].strip() if "," in line else ""
            name = (attrs.get("tvg-name") or display or "Unbekannt").strip()
            group = (attrs.get("group-title") or "Ohne Gruppe").strip()
            logo = (attrs.get("tvg-logo") or "").strip()
            url = ""
            cursor = index + 1
            while cursor < len(lines):
                candidate = lines[cursor].strip()
                if candidate and not candidate.startswith("#"):
                    url = candidate
                    break
                cursor += 1
            entries.append(
                {
                    "name": name,
                    "group": group,
                    "logo": logo,
                    "url": url,
                }
            )
            index = cursor
        index += 1
    return entries


def is_hls(text: str) -> bool:
    return text.lstrip("\ufeff \t\r\n").startswith("#EXTM3U")


def is_master_playlist(text: str) -> bool:
    return "#EXT-X-STREAM-INF" in text


def first_variant_url(text: str, base_url: str) -> str | None:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith("#EXT-X-STREAM-INF"):
            continue
        match = STREAM_INF_URI_RE.search(stripped)
        if match:
            return urllib.parse.urljoin(base_url, match.group(1).strip())
        for follower in lines[index + 1 :]:
            follower = follower.strip()
            if not follower:
                continue
            if follower.startswith("#"):
                if follower.startswith("#EXT-X-STREAM-INF") or follower.startswith(
                    "#EXT-X-I-FRAME-STREAM-INF"
                ):
                    break
                continue
            return urllib.parse.urljoin(base_url, follower)
    return None


def looks_like_ts(data: bytes) -> bool:
    if len(data) < 188:
        return False
    last = min(16, len(data) - 188)
    for offset in range(last + 1):
        if data[offset] != 0x47:
            continue
        second = offset + 188
        if second < len(data) and data[second] != 0x47:
            continue
        return True
    return False


def looks_like_media(data: bytes) -> bool:
    if len(data) < 12:
        return False
    if looks_like_ts(data):
        return True
    if data.startswith((b"FLV", b"OggS", b"RIFF", b"ID3", b"\x1a\x45\xdf\xa3")):
        return True
    if b"ftyp" in data[:64]:
        return True
    if data[0] == 0xFF and (data[1] & 0xE0) == 0xE0:
        return True
    return False


def looks_like_error_document(data: bytes) -> bool:
    sample = data.lstrip()[:240].lower()
    return sample.startswith(
        (
            b"<!doctype",
            b"<html",
            b"<head",
            b"<body",
            b"<?xml",
            b"{",
            b"[",
        )
    )


def classify_body(data: bytes) -> tuple[str, str]:
    """Gibt ('hls'|'media'|'other', text) zurück."""
    payload = data[3:] if data.startswith(b"\xef\xbb\xbf") else data
    stripped = payload.lstrip()
    if stripped.startswith(b"#EXTM3U"):
        return "hls", payload.decode("utf-8", "replace")
    if looks_like_error_document(payload):
        return "other", ""
    if looks_like_media(payload):
        return "media", ""
    return "other", ""


def maybe_decompress(data: bytes, content_encoding: str | None) -> bytes:
    """Manche Sender schicken Playlists als gzip, auch ohne Anfrage."""
    encoding = (content_encoding or "").lower()
    if data.startswith(b"\x1f\x8b") or "gzip" in encoding:
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as handle:
                return handle.read(MAX_BYTES)
        except Exception:
            return data
    if "deflate" in encoding:
        for args in ((), (-zlib.MAX_WBITS,)):
            try:
                return zlib.decompress(data, *args)[:MAX_BYTES]
            except Exception:
                continue
    return data


def fetch(url: str, timeout: float) -> tuple[int | None, bytes, str, str | None]:
    """Lädt höchstens MAX_BYTES. Gibt (http, daten, endadresse, fehler) zurück."""
    if not url.lower().startswith(("http://", "https://")):
        return None, b"", "", "keine http(s)-Adresse"
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Connection": "close",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(
            request, timeout=timeout, context=SSL_CONTEXT
        ) as response:
            code = int(response.getcode() or 0)
            final_url = response.geturl()
            data = maybe_decompress(
                response.read(MAX_BYTES),
                response.headers.get("Content-Encoding"),
            )
            return code, data, final_url, None
    except urllib.error.HTTPError as exc:
        return int(exc.code), b"", "", f"HTTP {exc.code}"
    except Exception as exc:
        message = str(exc).lower()
        if isinstance(exc, (TimeoutError, socket.timeout)) or "timed out" in message:
            return None, b"", "", "Zeitüberschreitung"
        return None, b"", "", "Verbindung fehlgeschlagen"


def evaluate(url: str, timeout: float) -> tuple[bool, int | None, str | None]:
    code, data, final_url, error = fetch(url, timeout)
    if error:
        return False, code, error
    if code != 200:
        return False, code, f"HTTP {code}" if code else "kein HTTP-Status"
    kind, text = classify_body(data)
    if kind == "media":
        return True, 200, None
    if kind != "hls":
        return False, 200, "Antwort ist kein HLS und keine Mediendaten"
    if not is_master_playlist(text):
        return True, 200, None
    variant = first_variant_url(text, final_url or url)
    if not variant:
        return False, 200, "Master-Playlist ohne Variante"
    variant_code, variant_data, _variant_url, variant_error = fetch(variant, timeout)
    if variant_error:
        return False, variant_code, f"Erste Variante: {variant_error}"
    if variant_code != 200:
        return (
            False,
            variant_code,
            f"Erste Variante: HTTP {variant_code}" if variant_code else "Erste Variante: kein HTTP-Status",
        )
    variant_kind, _variant_text = classify_body(variant_data)
    if variant_kind in {"hls", "media"}:
        return True, 200, None
    return False, 200, "Erste Variante ist kein HLS und keine Mediendaten"


def check_entry(index: int, entry: dict, timeout: float, total: int) -> dict:
    try:
        ok, code, error = evaluate(entry["url"], timeout)
    except Exception:
        ok, code, error = False, None, "Prüfung fehlgeschlagen"
    result = {
        "name": entry["name"],
        "group": entry["group"],
        "logo": entry["logo"],
        "ok": bool(ok),
        "http": code,
        "error": error,
        "checked_at": utc_now(),
        "_index": index,
    }
    with PRINT_LOCK:
        state = "OK" if ok else "FEHLER"
        detail = "" if ok else f" — {error or code or 'Fehler'}"
        print(f"[{index + 1}/{total}] {state} {entry['name']} ({entry['group']}){detail}", flush=True)
    return result


def german_stamp(iso_text: str) -> str:
    try:
        moment = datetime.strptime(iso_text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return iso_text
    return moment.strftime("%d.%m.%Y, %H:%M UTC")


def md_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def build_report(checked_at: str, entries: list[dict]) -> str:
    ok_count = sum(1 for entry in entries if entry["ok"])
    failed = [entry for entry in entries if not entry["ok"]]
    groups: list[str] = []
    stats: dict[str, list[int]] = {}
    for entry in entries:
        group = entry["group"]
        if group not in stats:
            groups.append(group)
            stats[group] = [0, 0]
        stats[group][0 if entry["ok"] else 1] += 1

    lines = [
        "# Stream-Bericht",
        "",
        f"**Datum:** {german_stamp(checked_at)}",
        "",
        f"**Geprüft:** {len(entries)} Sender · **OK:** {ok_count} · **Fehler:** {len(failed)}",
        "",
        "## Ergebnis nach Gruppe",
        "",
        "| Gruppe | OK | Fehler |",
        "| --- | ---: | ---: |",
    ]
    for group in groups:
        ok_n, bad_n = stats[group]
        lines.append(f"| {md_cell(group)} | {ok_n} | {bad_n} |")
    lines.extend(["", "## Fehlerhafte Sender", ""])
    if not failed:
        lines.append("Keine fehlerhaften Sender.")
    else:
        for entry in failed:
            reason = entry["error"] or (f"HTTP {entry['http']}" if entry["http"] else "Fehler")
            lines.append(
                f"- **{entry['name']}** ({entry['group']}) — {reason}"
            )
    lines.extend(
        [
            "",
            "---",
            "",
            "Hinweis: Die Prüfung läuft auf den Servern von GitHub in den USA. "
            "Manche Sender, die außerhalb von Russland oder Deutschland gesperrt sind, "
            "können hier als fehlerhaft erscheinen, obwohl sie zu Hause funktionieren.",
            "",
        ]
    )
    return "\n".join(lines)


def assert_safe_output(path: Path) -> None:
    resolved = path.resolve()
    if resolved.name == PLAYLIST_NAME or resolved.suffix.lower() in {".m3u", ".m3u8"}:
        raise SystemExit(f"Ausgabe darf keine Playlist sein ({resolved.name}).")


def write_outputs(status_path: Path, report_path: Path, entries: list[dict]) -> str:
    assert_safe_output(status_path)
    assert_safe_output(report_path)
    checked_at = utc_now()
    public_entries = []
    for entry in entries:
        public_entries.append(
            {
                "name": entry["name"],
                "group": entry["group"],
                "logo": entry["logo"],
                "ok": entry["ok"],
                "http": entry["http"],
                "error": entry["error"],
                "checked_at": entry["checked_at"],
            }
        )
    ok_count = sum(1 for entry in public_entries if entry["ok"])
    payload = {
        "checked_at": checked_at,
        "total": len(public_entries),
        "ok": ok_count,
        "failed": len(public_entries) - ok_count,
        "entries": public_entries,
    }
    status_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    report_path.write_text(build_report(checked_at, public_entries), encoding="utf-8")
    return checked_at


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prüft die DomaTV-Streams.")
    parser.add_argument("--playlist", type=Path, default=DEFAULT_PLAYLIST)
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument("--timeout", type=float, default=TIMEOUT_SECONDS)
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Nur die ersten N Sender prüfen (0 = alle).",
    )
    args = parser.parse_args(argv)

    playlist_path = args.playlist
    if not playlist_path.is_file():
        print(f"Playlist nicht gefunden: {playlist_path.name}", file=sys.stderr)
        return 1
    # Die Playlist ausdrücklich nur lesen.
    text = playlist_path.read_text(encoding="utf-8")
    entries = parse_playlist(text)
    if args.limit and args.limit > 0:
        entries = entries[: args.limit]
    if not entries:
        print("Keine Sender in der Playlist gefunden.", file=sys.stderr)
        return 1

    workers = max(1, min(args.workers, len(entries)))
    print(
        f"Prüfe {len(entries)} Sender mit {workers} Verbindungen, "
        f"Timeout {args.timeout:.0f}s …",
        flush=True,
    )
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(check_entry, index, entry, args.timeout, len(entries))
            for index, entry in enumerate(entries)
        ]
        for future in as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda item: item["_index"])
    for item in results:
        item.pop("_index", None)

    write_outputs(args.status, args.report, results)
    ok_count = sum(1 for entry in results if entry["ok"])
    print(f"Geprüft: {len(results)}", flush=True)
    print(f"OK: {ok_count}", flush=True)
    print(f"Fehler: {len(results) - ok_count}", flush=True)
    return 0


def _self_test() -> None:
    sample = (
        '#EXTM3U\n'
        '#EXTINF:-1 tvg-name="A" tvg-logo="https://example.com/a.png" '
        'group-title="Gruppe",A\n'
        "https://example.com/hidden.m3u8\n"
    )
    parsed = parse_playlist(sample)
    assert len(parsed) == 1
    assert parsed[0]["name"] == "A"
    assert parsed[0]["group"] == "Gruppe"
    assert parsed[0]["logo"] == "https://example.com/a.png"
    master = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\nlow.m3u8\n"
    assert first_variant_url(master, "https://cdn.example/live/master.m3u8") == (
        "https://cdn.example/live/low.m3u8"
    )
    assert is_hls("#EXTM3U\n#EXTINF:1,\nseg.ts\n")
    assert looks_like_media(b"\x47" + b"\x00" * 187 + b"\x47" + b"\x00" * 20)
    kind, _text = classify_body(b"<!DOCTYPE html><html><body>no</body></html>")
    assert kind == "other"
    kind, text = classify_body(b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nv.m3u8\n")
    assert kind == "hls" and is_master_playlist(text)
    compressed = gzip.compress(b"#EXTM3U\n#EXTINF:1,\nseg.ts\n")
    plain = maybe_decompress(compressed, "gzip")
    kind, _text = classify_body(plain)
    assert kind == "hls"
    report = build_report(
        "2026-11-02T04:00:00Z",
        [
            {
                "name": "Gut",
                "group": "Gruppe",
                "logo": "",
                "ok": True,
                "http": 200,
                "error": None,
                "checked_at": "2026-11-02T04:00:00Z",
            },
            {
                "name": "Kaputt",
                "group": "Gruppe",
                "logo": "",
                "ok": False,
                "http": 404,
                "error": "HTTP 404",
                "checked_at": "2026-11-02T04:00:00Z",
            },
        ],
    )
    assert "Kaputt" in report
    assert "https://" not in report
    assert "USA" in report
    print("self-test ok")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--self-test":
        _self_test()
        raise SystemExit(0)
    raise SystemExit(run())
