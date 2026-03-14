"""QR code generation for customers and tables."""

from __future__ import annotations

import base64
import io


def _make_qr(url: str) -> bytes:
    """Return PNG bytes for a QR code encoding *url*."""
    import qrcode
    from qrcode.image.pil import PilImage

    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=4,
    )
    qr.add_data(url)
    qr.make(fit=True)
    img: PilImage = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def generate_customer_qr(uid_token: str, base_url: str) -> bytes:
    """Return PNG bytes for a personal customer QR code."""
    url = f"{base_url.rstrip('/')}/?uid={uid_token}"
    return _make_qr(url)


def generate_table_qr(
    table_token: str,
    base_url: str,
    table_number: str,
) -> bytes:
    """Return PNG bytes for a table QR code."""
    url = f"{base_url.rstrip('/')}/?table={table_token}"
    return _make_qr(url)


def generate_combined_qr(
    uid_token: str,
    table_token: str,
    base_url: str,
) -> bytes:
    """Return PNG bytes for a combined customer+table QR code."""
    url = f"{base_url.rstrip('/')}/?uid={uid_token}&table={table_token}"
    return _make_qr(url)


def qr_to_base64(qr_bytes: bytes) -> str:
    """Encode QR PNG bytes as a base64 string (for ``st.image``)."""
    return base64.b64encode(qr_bytes).decode("utf-8")
