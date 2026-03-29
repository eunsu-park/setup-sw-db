"""Data download functions."""
import re
from pathlib import Path

import requests
import urllib3

# Suppress SSL warnings for JSOC self-signed certificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def download(url: str, timeout: int = 30, max_retries: int = 3) -> str | None:
    """Download text data from URL.

    Args:
        url: URL to download from.
        timeout: Request timeout in seconds.
        max_retries: Maximum number of retry attempts.

    Returns:
        Response text or None if download failed.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            if not response.text.strip():
                print(f"  Empty response from {url}")
                return None
            return response.text
        except requests.RequestException as e:
            is_retriable = not isinstance(e, requests.HTTPError) or \
                           (e.response is not None and e.response.status_code >= 500)
            if is_retriable and attempt < max_retries - 1:
                print(f"  Retry {attempt + 1}/{max_retries}: {e}")
            else:
                print(f"  Download failed: {e}")
                return None
    return None


def download_file(url: str, save_path: str, timeout: int = 600,
                  overwrite: bool = False, max_retries: int = 3,
                  verify_ssl: bool = False) -> bool:
    """Download binary file from URL and save to disk.

    Args:
        url: URL to download from.
        save_path: Local path to save the file.
        timeout: Request timeout in seconds.
        overwrite: If True, overwrite existing files.
        max_retries: Maximum number of retry attempts.
        verify_ssl: If True, verify SSL certificates. Defaults to False
            for JSOC self-signed certificates.

    Returns:
        True if download succeeded, False otherwise.
    """
    path = Path(save_path)

    if path.exists() and not overwrite:
        return True

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout, stream=True, verify=verify_ssl)
            response.raise_for_status()

            # Get expected file size from Content-Length header
            expected_size = response.headers.get('Content-Length')
            if expected_size:
                expected_size = int(expected_size)

            path.parent.mkdir(parents=True, exist_ok=True)

            downloaded_size = 0
            with open(path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1048576):
                    f.write(chunk)
                    downloaded_size += len(chunk)

            # Verify file size if Content-Length was provided
            if expected_size and downloaded_size != expected_size:
                print(f"  Incomplete download ({downloaded_size}/{expected_size} bytes), "
                      f"retry {attempt + 1}/{max_retries}")
                path.unlink(missing_ok=True)
                continue

            return True

        except requests.RequestException as e:
            print(f"  Download failed (attempt {attempt + 1}/{max_retries}): {e}")
            path.unlink(missing_ok=True)

    return False


def list_remote_files(url: str, extension: str = ".fts") -> list[str]:
    """Parse directory listing from URL and return file names.

    Args:
        url: URL of directory listing page.
        extension: File extension to filter (e.g., ".fts").

    Returns:
        List of file names matching the extension.
    """
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        pattern = rf'href="([^"]+{re.escape(extension)})"'
        files = re.findall(pattern, response.text, re.IGNORECASE)

        return sorted(set(files))
    except requests.RequestException as e:
        print(f"  Failed to list files: {e}")
        return []


def download_files_parallel(tasks: list[tuple[str, str]], max_workers: int = 4,
                            overwrite: bool = False) -> tuple[int, int]:
    """Download multiple files in parallel.

    Args:
        tasks: List of (url, save_path) tuples.
        max_workers: Maximum number of parallel downloads.
        overwrite: If True, overwrite existing files.

    Returns:
        Tuple of (success_count, failed_count).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not tasks:
        return 0, 0

    success = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_download_task, url, path, overwrite): (url, path)
            for url, path in tasks
        }

        for future in as_completed(futures):
            if future.result():
                success += 1
            else:
                failed += 1

    return success, failed


def _download_task(url: str, save_path: str, overwrite: bool) -> bool:
    """Single download task for parallel execution.

    Args:
        url: URL to download from.
        save_path: Local path to save the file.
        overwrite: If True, overwrite existing files.

    Returns:
        True if download succeeded, False otherwise.
    """
    return download_file(url, save_path, overwrite=overwrite)