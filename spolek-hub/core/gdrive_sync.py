"""
Google Drive synchronisation for Spolkový Hospodský Systém.

Two implementations are provided:
  GDriveSync    – real Google Drive via Service Account
  MockGDriveSync – local-only stub for development / testing

Use get_sync_manager() to obtain the appropriate instance.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import socket
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DB_FILENAME = "spolek_pos.db"
LOCK_FILENAME = "spolek_pos.lock"


# ---------------------------------------------------------------------------
# Real Google Drive implementation
# ---------------------------------------------------------------------------

class GDriveSync:
    """Synchronises a DuckDB file with a Google Drive folder.

    Lifecycle::

        sync = GDriveSync(credentials_dict, folder_id)
        db_path, device_id = sync.initialize()
        # … use db_path …
        sync.mark_dirty()   # after every write
        sync.release()      # on shutdown
    """

    def __init__(self, credentials_dict: dict, folder_id: str) -> None:
        self._creds_dict = credentials_dict
        self._folder_id = folder_id
        self._db_path: Optional[Path] = None
        self._dirty: bool = False
        self._last_sync: Optional[datetime] = None
        self._online: bool = False
        self._device_id: str = self._get_device_id()
        self._bg_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._service = None  # google drive service object

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def initialize(self) -> tuple[Path, str]:
        """Ping Drive, acquire lock, download DB, start background sync.

        Returns:
            (local_db_path, device_id)
        """
        self._online = self._ping_drive()
        if not self._online:
            logger.warning("Google Drive not reachable – running in offline mode")
            self._db_path = Path(tempfile.gettempdir()) / DB_FILENAME
            return self._db_path, self._device_id

        # Check for stale / foreign lock
        lock_info = self._read_lock()
        if lock_info and not self._is_stale_lock(lock_info):
            holder = lock_info.get("device_id", "unknown")
            raise RuntimeError(
                f"Databáze je zamčena jiným zařízením ({holder}). "
                "Počkejte nebo odstraňte lock soubor ručně."
            )

        self._db_path = Path(tempfile.gettempdir()) / DB_FILENAME
        self._download_db()
        self._write_lock()
        self._start_background_sync(interval_sec=30)
        return self._db_path, self._device_id

    def mark_dirty(self) -> None:
        """Signal that the local DB has unsynchronised changes."""
        self._dirty = True

    def force_sync(self) -> bool:
        """Upload local DB to Drive immediately.

        Returns:
            True if upload succeeded, False otherwise.
        """
        if not self._online or self._db_path is None:
            return False
        ok = self._upload_db()
        if ok:
            self._dirty = False
            self._last_sync = datetime.now(timezone.utc)
        return ok

    def release(self) -> None:
        """Upload final state, create daily backup, remove lock."""
        self._stop_event.set()
        if self._bg_thread:
            self._bg_thread.join(timeout=10)

        if self._online and self._db_path and self._db_path.exists():
            self._upload_db()
            self._create_daily_backup()
            self._cleanup_old_backups(keep_days=7)
            self._delete_lock()

    @property
    def status(self) -> dict:
        """Return a status dict for the UI indicator."""
        return {
            "online": self._online,
            "dirty": self._dirty,
            "last_sync": self._last_sync,
        }

    # ------------------------------------------------------------------
    # Private – Drive operations
    # ------------------------------------------------------------------

    def _get_service(self):
        """Build and cache a Google Drive service object."""
        if self._service is not None:
            return self._service
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        creds = service_account.Credentials.from_service_account_info(
            self._creds_dict,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        self._service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return self._service

    def _ping_drive(self) -> bool:
        """Return True if we can reach Google Drive."""
        try:
            socket.setdefaulttimeout(5)
            socket.getaddrinfo("www.googleapis.com", 443)
            # also verify credentials work
            svc = self._get_service()
            svc.files().list(pageSize=1, fields="files(id)").execute()
            return True
        except Exception as exc:
            logger.warning("Drive ping failed: %s", exc)
            return False

    def _find_file(self, name: str) -> Optional[str]:
        """Return Drive file ID for *name* in the configured folder, or None."""
        try:
            svc = self._get_service()
            q = (
                f"name='{name}' and "
                f"'{self._folder_id}' in parents and "
                "trashed=false"
            )
            res = svc.files().list(q=q, fields="files(id,name)").execute()
            files = res.get("files", [])
            return files[0]["id"] if files else None
        except Exception as exc:
            logger.error("_find_file(%s) failed: %s", name, exc)
            return None

    def _download_db(self) -> bool:
        """Download DB from Drive to local /tmp/. Returns True on success."""
        assert self._db_path is not None
        file_id = self._find_file(DB_FILENAME)
        if file_id is None:
            logger.info("No existing DB on Drive – starting fresh")
            return False
        try:
            from googleapiclient.http import MediaIoBaseDownload
            import io

            svc = self._get_service()
            request = svc.files().get_media(fileId=file_id)
            fh = io.FileIO(str(self._db_path), "wb")
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            logger.info("DB downloaded from Drive (%s)", self._db_path)
            self._last_sync = datetime.now(timezone.utc)
            return True
        except Exception as exc:
            logger.error("_download_db failed: %s", exc)
            return False

    def _upload_db(self) -> bool:
        """Upload local DB to Drive. Returns True on success."""
        assert self._db_path is not None
        if not self._db_path.exists():
            return False
        try:
            from googleapiclient.http import MediaFileUpload

            svc = self._get_service()
            media = MediaFileUpload(str(self._db_path), mimetype="application/octet-stream")
            file_id = self._find_file(DB_FILENAME)
            if file_id:
                svc.files().update(fileId=file_id, media_body=media).execute()
            else:
                metadata = {"name": DB_FILENAME, "parents": [self._folder_id]}
                svc.files().create(body=metadata, media_body=media).execute()
            self._last_sync = datetime.now(timezone.utc)
            logger.info("DB uploaded to Drive")
            return True
        except Exception as exc:
            logger.error("_upload_db failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Private – Lock
    # ------------------------------------------------------------------

    def _write_lock(self) -> None:
        """Create a lock file on Drive."""
        try:
            import io
            from googleapiclient.http import MediaIoBaseUpload

            info = json.dumps({
                "device_id": self._device_id,
                "hostname": socket.gethostname(),
                "locked_at": datetime.now(timezone.utc).isoformat(),
            }).encode()
            svc = self._get_service()
            media = MediaIoBaseUpload(io.BytesIO(info), mimetype="application/json")
            existing_id = self._find_file(LOCK_FILENAME)
            if existing_id:
                svc.files().update(fileId=existing_id, media_body=media).execute()
            else:
                metadata = {"name": LOCK_FILENAME, "parents": [self._folder_id]}
                svc.files().create(body=metadata, media_body=media).execute()
        except Exception as exc:
            logger.warning("_write_lock failed: %s", exc)

    def _delete_lock(self) -> None:
        """Remove lock file from Drive."""
        try:
            svc = self._get_service()
            file_id = self._find_file(LOCK_FILENAME)
            if file_id:
                svc.files().delete(fileId=file_id).execute()
        except Exception as exc:
            logger.warning("_delete_lock failed: %s", exc)

    def _read_lock(self) -> Optional[dict]:
        """Download and parse lock file. Returns None if no lock exists."""
        try:
            import io
            svc = self._get_service()
            file_id = self._find_file(LOCK_FILENAME)
            if not file_id:
                return None
            data = svc.files().get_media(fileId=file_id).execute()
            return json.loads(data)
        except Exception as exc:
            logger.warning("_read_lock failed: %s", exc)
            return None

    def _is_stale_lock(self, info: dict, timeout_min: int = 30) -> bool:
        """Return True if the lock was written more than *timeout_min* ago."""
        try:
            locked_at_str = info.get("locked_at", "")
            locked_at = datetime.fromisoformat(locked_at_str)
            age_min = (datetime.now(timezone.utc) - locked_at).total_seconds() / 60
            return age_min > timeout_min
        except Exception:
            return True  # malformed lock → treat as stale

    # ------------------------------------------------------------------
    # Private – Backup
    # ------------------------------------------------------------------

    def _create_daily_backup(self) -> None:
        """Copy DB on Drive to a dated backup file."""
        assert self._db_path is not None
        try:
            backup_name = (
                f"spolek_pos_backup_{datetime.now(timezone.utc).date().isoformat()}.db"
            )
            # Skip if today's backup already exists
            if self._find_file(backup_name):
                return
            from googleapiclient.http import MediaFileUpload

            svc = self._get_service()
            media = MediaFileUpload(str(self._db_path), mimetype="application/octet-stream")
            metadata = {"name": backup_name, "parents": [self._folder_id]}
            svc.files().create(body=metadata, media_body=media).execute()
            logger.info("Daily backup created: %s", backup_name)
        except Exception as exc:
            logger.warning("_create_daily_backup failed: %s", exc)

    def _cleanup_old_backups(self, keep_days: int = 7) -> None:
        """Delete backup files older than *keep_days*."""
        try:
            svc = self._get_service()
            q = (
                f"name contains 'spolek_pos_backup_' and "
                f"'{self._folder_id}' in parents and "
                "trashed=false"
            )
            res = svc.files().list(
                q=q, fields="files(id,name,createdTime)", orderBy="createdTime"
            ).execute()
            backups = res.get("files", [])
            cutoff = time.time() - keep_days * 86400
            for b in backups:
                created = datetime.fromisoformat(
                    b["createdTime"].replace("Z", "+00:00")
                ).timestamp()
                if created < cutoff:
                    svc.files().delete(fileId=b["id"]).execute()
                    logger.info("Deleted old backup: %s", b["name"])
        except Exception as exc:
            logger.warning("_cleanup_old_backups failed: %s", exc)

    # ------------------------------------------------------------------
    # Private – Background sync
    # ------------------------------------------------------------------

    def _start_background_sync(self, interval_sec: int = 30) -> None:
        """Start a daemon thread that uploads dirty DB every *interval_sec*."""

        def _loop() -> None:
            while not self._stop_event.is_set():
                self._stop_event.wait(timeout=interval_sec)
                if self._dirty and self._online:
                    ok = self._upload_db()
                    if ok:
                        self._dirty = False

        self._bg_thread = threading.Thread(target=_loop, daemon=True, name="gdrive-sync")
        self._bg_thread.start()

    # ------------------------------------------------------------------
    # Private – Device ID
    # ------------------------------------------------------------------

    def _get_device_id(self) -> str:
        """Return an 8-char MD5 hash of the hostname."""
        return hashlib.md5(socket.gethostname().encode()).hexdigest()[:8]


# ---------------------------------------------------------------------------
# Mock implementation for local development
# ---------------------------------------------------------------------------

class MockGDriveSync:
    """Local-only stub – same API as GDriveSync, no Drive calls."""

    def __init__(self, local_db_path: Optional[str] = None) -> None:
        self._db_path = Path(local_db_path) if local_db_path else (
            Path(tempfile.gettempdir()) / DB_FILENAME
        )
        self._dirty = False
        self._last_sync: Optional[datetime] = None
        self._device_id = hashlib.md5(socket.gethostname().encode()).hexdigest()[:8]

    def initialize(self) -> tuple[Path, str]:
        """Return local DB path; no network operations."""
        logger.info("MockGDriveSync: using local file %s", self._db_path)
        return self._db_path, self._device_id

    def mark_dirty(self) -> None:
        self._dirty = True

    def force_sync(self) -> bool:
        """No-op – always reports success."""
        self._dirty = False
        self._last_sync = datetime.now(timezone.utc)
        return True

    def release(self) -> None:
        """No-op."""
        self._dirty = False

    @property
    def status(self) -> dict:
        return {
            "online": False,
            "dirty": self._dirty,
            "last_sync": self._last_sync,
        }


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_sync_manager() -> GDriveSync | MockGDriveSync:
    """Return the appropriate sync manager based on available configuration.

    Checks (in order):
      1. Streamlit secrets (``GDRIVE_FOLDER_ID`` + ``GDRIVE_CREDENTIALS_JSON``)
      2. Environment variables (same names, via python-dotenv)
      3. Falls back to MockGDriveSync with a warning.
    """
    # Load .env if present (no-op in production)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    folder_id: Optional[str] = None
    creds_dict: Optional[dict] = None

    # 1. Streamlit secrets
    try:
        import streamlit as st
        folder_id = st.secrets.get("GDRIVE_FOLDER_ID")
        raw_creds = st.secrets.get("GDRIVE_CREDENTIALS_JSON")
        if raw_creds:
            creds_dict = json.loads(raw_creds) if isinstance(raw_creds, str) else dict(raw_creds)
    except Exception:
        pass

    # 2. Environment variables
    if not folder_id:
        folder_id = os.getenv("GDRIVE_FOLDER_ID")
    if not creds_dict:
        raw_env = os.getenv("GDRIVE_CREDENTIALS_JSON", "")
        if raw_env:
            try:
                creds_dict = json.loads(raw_env)
            except json.JSONDecodeError:
                logger.warning("GDRIVE_CREDENTIALS_JSON is not valid JSON")

    if folder_id and creds_dict:
        return GDriveSync(creds_dict, folder_id)

    logger.warning(
        "GDRIVE_FOLDER_ID nebo GDRIVE_CREDENTIALS_JSON není nastaveno. "
        "Spouštím v offline režimu (MockGDriveSync)."
    )
    return MockGDriveSync()
