"""Intuit authenticator challenges. Never log the enrollment secret or OTP."""

import base64
import binascii
import hashlib
import hmac
import re
import struct
import time
from urllib.parse import urlsplit


class AuthenticatorError(RuntimeError):
    """A safe-to-log authentication failure without page or credential data."""


def _decode_secret(secret):
    normalized = "".join(secret.split()).upper().rstrip("=")
    if not normalized or not re.fullmatch(r"[A-Z2-7]+", normalized):
        raise AuthenticatorError("QB_TOTP_SECRET must be a Base32 enrollment key")
    try:
        decoded = base64.b32decode(normalized + "=" * (-len(normalized) % 8))
    except (binascii.Error, ValueError):
        raise AuthenticatorError("QB_TOTP_SECRET is not a valid Base32 enrollment key") from None
    if len(decoded) < 20:
        raise AuthenticatorError("QB_TOTP_SECRET must contain at least 160 bits")
    return decoded


def generate_totp(secret, at_time=None):
    """RFC 6238 SHA-1, 30-second steps, six digits (Intuit enrollment)."""
    key = _decode_secret(secret)
    timestamp = time.time() if at_time is None else at_time
    if timestamp < 0:
        raise AuthenticatorError("TOTP requires a nonnegative Unix timestamp")
    digest = hmac.new(key, struct.pack(">Q", int(timestamp // 30)), hashlib.sha1).digest()
    offset = digest[-1] & 15
    number = struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7fffffff
    return f"{number % 1_000_000:06d}"


def _signed_in(page):
    parsed = urlsplit(page.url)
    return parsed.scheme == "https" and parsed.hostname == "qbo.intuit.com" and parsed.path.startswith("/app/")


def _require_login_page(page):
    parsed = urlsplit(page.url)
    if (parsed.scheme != "https" or parsed.hostname != "accounts.intuit.com"
            or "account-manager" in parsed.path):
        raise AuthenticatorError("TOTP_UNEXPECTED_LOGIN_ORIGIN")


def _click_method(page, pattern):
    for role in ("button", "link", "radio"):
        for control in page.get_by_role(role, name=re.compile(pattern, re.I)).all():
            if control.is_visible() and control.is_enabled():
                control.click()
                return True
    return False


def complete_totp_challenge(page, secret, *, clock=time.time, sleep=time.sleep):
    """Resolve only authenticator challenges; stop rather than silently send SMS.

    Called after password submission. It never changes Intuit account settings,
    enrolls an authenticator, or submits codes to bank-provider domains.
    Exactly one code is submitted, with a short rollover buffer.
    """
    _decode_secret(secret)
    if _signed_in(page):
        return
    _require_login_page(page)
    text = page.inner_text("body").lower()
    if "captcha" in text or "robot" in text:
        raise AuthenticatorError("TOTP_INTERACTIVE_CHALLENGE_REQUIRED")

    # Observed Intuit route: SMS challenge -> "I didn't get a text message"
    # -> "See more options" -> method chooser. Never select resend/voice or
    # the separate identity-recovery process labeled "takes longer".
    alternate = (
        r"(try|use|choose).*(another|different|other).*(way|method|option)|"
        r"more options|try something else|"
        r"i (?:didn't|didn’t|did not) get (?:a |the )?(?:text message|code)"
    )
    for _ in range(3):
        _require_login_page(page)
        if _click_method(page, r"authenticator"):
            sleep(1)
            break
        if "authenticator" in page.inner_text("body").lower():
            break
        if not _click_method(page, alternate):
            break
        sleep(1)
        if _signed_in(page):
            return
    if _signed_in(page):
        return
    _require_login_page(page)
    text = page.inner_text("body").lower()
    # A generated TOTP must never be entered into an SMS/email-code form.
    if not re.search(
        r"(?:code.{0,80}(?:from|in|using).{0,50}authenticator|"
        r"authenticator.{0,80}code|open.{0,80}authenticator)", text, re.S
    ) or re.search(r"(?:we(?:'ve| have)? sent|we texted).{0,100}(?:phone|email|text)", text, re.S):
        raise AuthenticatorError("TOTP_METHOD_UNAVAILABLE")

    fields = [field for field in page.locator(
        'input[autocomplete="one-time-code"], input[inputmode="numeric"], '
        'input[type="tel"], input[name="otp"], input[aria-label*="code" i], '
        'input[id*="otp" i]'
    ).all() if field.is_visible() and field.is_enabled()]
    if len(fields) != 1 and not (
        len(fields) == 6 and all(field.get_attribute("maxlength") == "1" for field in fields)
    ):
        raise AuthenticatorError("TOTP_CODE_INPUT_UNSUPPORTED")

    # Do not start typing a code which is about to expire.
    remaining = 30 - clock() % 30
    if remaining < 5:
        sleep(remaining + 0.5)
    _require_login_page(page)
    code = generate_totp(secret, clock())
    try:
        if len(fields) == 1:
            fields[0].fill(code)
        else:
            for field, digit in zip(fields, code):
                field.fill(digit)
        if _signed_in(page):  # Some forms submit as soon as the last digit is filled.
            return
        _require_login_page(page)
        if not _click_method(page, r"^(continue|verify|verify code|sign in|submit)$"):
            raise AuthenticatorError("TOTP_SUBMIT_UNAVAILABLE")
    except AuthenticatorError:
        raise
    except Exception:
        # Playwright exceptions can include the value passed to fill().
        raise AuthenticatorError("TOTP_SUBMISSION_FAILED") from None
    try:
        page.wait_for_url("https://qbo.intuit.com/app/**", timeout=60000)
    except Exception:
        if not _signed_in(page):
            raise AuthenticatorError("TOTP_VERIFICATION_FAILED") from None
    if not _signed_in(page):
        raise AuthenticatorError("TOTP_VERIFICATION_FAILED")
