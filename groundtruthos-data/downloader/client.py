"""
Dataset download client with rate limiting, retries, and progress tracking.
"""
import hashlib
import time
import logging
from pathlib import Path
from threading import Lock

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe token bucket rate limiter."""

    def __init__(self, requests_per_second: float = 1.0):
        self.min_interval = 1.0 / requests_per_second
        self._last_request_time = 0.0
        self._lock = Lock()

    def wait(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
            self._last_request_time = time.monotonic()


class DatasetClient:
    """HTTP client for downloading geospatial dataset files.

    Features:
    - Automatic retries with exponential backoff
    - Rate limiting (configurable per source)
    - Resume support for partial downloads
    - Progress bar via tqdm
    - SHA256 checksum computation on download
    """

    DEFAULT_CHUNK_SIZE = 64 * 1024  # 64KB chunks
    DEFAULT_TIMEOUT = 300  # 5 minutes

    def __init__(
        self,
        rate_limit_rps: float = 1.0,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        self.rate_limiter = RateLimiter(rate_limit_rps)
        self.timeout = timeout

        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def download_file(
        self,
        file_url: str,
        dest_path: Path,
        expected_checksum: str | None = None,
        resume: bool = True,
    ) -> dict:
        """Download a file with progress tracking and optional resume.

        Args:
            file_url: URL to download.
            dest_path: Local destination path.
            expected_checksum: Optional SHA256 hex digest for validation.
            resume: If True, attempt to resume partial downloads.

        Returns:
            Dict with download metadata:
                - file_path: Path to downloaded file
                - size_bytes: File size
                - sha256: Computed checksum
                - resumed: Whether download was resumed
                - elapsed_seconds: Download time
        """
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        start_time = time.monotonic()

        headers = {}
        existing_size = 0
        resumed = False

        if resume and dest_path.exists():
            existing_size = dest_path.stat().st_size
            headers["Range"] = f"bytes={existing_size}-"
            resumed = True

        self.rate_limiter.wait()
        response = self.session.get(
            file_url,
            stream=True,
            timeout=self.timeout,
            headers=headers,
        )

        # If range not supported, start fresh
        if resumed and response.status_code != 206:
            existing_size = 0
            resumed = False
            response = self.session.get(
                file_url, stream=True, timeout=self.timeout
            )

        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        if resumed:
            total_size += existing_size

        sha256 = hashlib.sha256()
        mode = "ab" if resumed else "wb"

        # If resuming, hash existing content first
        if resumed and existing_size > 0:
            with open(dest_path, "rb") as f:
                for chunk in iter(lambda: f.read(self.DEFAULT_CHUNK_SIZE), b""):
                    sha256.update(chunk)

        with open(dest_path, mode) as f, tqdm(
            desc=dest_path.name,
            total=total_size,
            initial=existing_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as progress:
            for chunk in response.iter_content(chunk_size=self.DEFAULT_CHUNK_SIZE):
                if chunk:
                    written = f.write(chunk)
                    sha256.update(chunk)
                    progress.update(written)

        computed_checksum = sha256.hexdigest()
        elapsed = time.monotonic() - start_time

        if expected_checksum and computed_checksum != expected_checksum:
            dest_path.unlink(missing_ok=True)
            raise ChecksumError(
                f"Checksum mismatch for {dest_path.name}: "
                f"expected {expected_checksum}, got {computed_checksum}"
            )

        return {
            "file_path": str(dest_path),
            "size_bytes": dest_path.stat().st_size,
            "sha256": computed_checksum,
            "resumed": resumed,
            "elapsed_seconds": round(elapsed, 2),
        }

    def head(self, url: str) -> dict:
        """Get headers for a URL (useful for checking file size before download)."""
        self.rate_limiter.wait()
        response = self.session.head(url, timeout=30, allow_redirects=True)
        response.raise_for_status()
        return dict(response.headers)

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class ChecksumError(Exception):
    pass
