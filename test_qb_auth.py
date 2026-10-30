"""Offline authentication tests; no live credentials, browser, or bank writes."""

import base64
import unittest
from unittest.mock import patch

from qb_auth import AuthenticatorError, complete_totp_challenge, generate_totp


# Public RFC 6238 Appendix B test key, not an account credential.
RFC_KEY = base64.b32encode(b"12345678901234567890").decode()


class Control:
    def __init__(self, page, name, role="button", maxlength=None, on_click=None):
        self.page, self.name, self.role = page, name, role
        self.maxlength, self.on_click = maxlength, on_click

    def is_visible(self):
        return True

    def is_enabled(self):
        return True

    def click(self):
        self.page.clicked.append(self.name)
        if self.on_click:
            self.on_click()

    def fill(self, value):
        self.page.entered.append(value)

    def get_attribute(self, name):
        return self.maxlength if name == "maxlength" else None


class Collection:
    def __init__(self, controls):
        self.controls = controls

    def all(self):
        return self.controls


class LoginPage:
    def __init__(self, text="Enter the code from your authenticator app", segmented=False):
        self.url = "https://accounts.intuit.com/app/sign-in"
        self.text = text
        self.entered, self.clicked = [], []
        self.fields = [Control(self, "otp", maxlength="1" if segmented else None)
                       for _ in range(6 if segmented else 1)]
        self.controls = [Control(self, "Continue")]
        self.reject = False

    def inner_text(self, selector):
        return self.text

    def get_by_role(self, role, name):
        return Collection([c for c in self.controls if c.role == role and name.search(c.name)])

    def locator(self, selector):
        return Collection(self.fields)

    def wait_for_url(self, pattern, timeout):
        if self.reject:
            raise TimeoutError("Login did not finish")
        self.url = "https://qbo.intuit.com/app/homepage"


class TotpTests(unittest.TestCase):
    def test_rfc_6238_sha1_vectors_with_six_digit_truncation(self):
        # RFC publishes eight digits; six digits use the same value modulo 10**6.
        for timestamp, expected in [
            (59, "287082"), (1111111109, "081804"),
            (1111111111, "050471"), (1234567890, "005924"),
            (2000000000, "279037"), (20000000000, "353130"),
        ]:
            with self.subTest(timestamp=timestamp):
                self.assertEqual(generate_totp(RFC_KEY, timestamp), expected)

    def test_whitespace_case_and_padding(self):
        grouped = " ".join(RFC_KEY.lower()[i:i + 4] for i in range(0, len(RFC_KEY), 4))
        self.assertEqual(generate_totp(grouped + "=", 59), "287082")

    def test_invalid_secret_errors_do_not_echo_input(self):
        for value in ("", "private-invalid-secret!", "A", "MY======", "otpauth://totp/private"):
            with self.subTest(kind=len(value)):
                with self.assertRaises(AuthenticatorError) as ctx:
                    generate_totp(value, 59)
                if value:
                    self.assertNotIn(value, str(ctx.exception))

    def test_counter_changes_at_boundary(self):
        self.assertEqual(generate_totp(RFC_KEY, 30), generate_totp(RFC_KEY, 59.99))
        self.assertNotEqual(generate_totp(RFC_KEY, 59.99), generate_totp(RFC_KEY, 60))


class ChallengeTests(unittest.TestCase):
    def run_challenge(self, page, clock=lambda: 45, sleep=lambda seconds: None):
        complete_totp_challenge(page, RFC_KEY, clock=clock, sleep=sleep)

    def test_direct_challenge_logs_in_with_one_code(self):
        page = LoginPage()
        self.run_challenge(page)
        self.assertEqual(page.entered, [generate_totp(RFC_KEY, 45)])
        self.assertEqual(page.clicked, ["Continue"])
        self.assertEqual(page.url, "https://qbo.intuit.com/app/homepage")

    def test_six_separate_code_fields(self):
        page = LoginPage(segmented=True)
        self.run_challenge(page)
        self.assertEqual("".join(page.entered), generate_totp(RFC_KEY, 45))

    def test_selects_authenticator_from_alternate_methods(self):
        page = LoginPage("We sent a text message to your phone")

        def show_authenticator():
            page.text = "Enter the code from your authenticator app"
            page.controls = [Control(page, "Continue")]

        def show_methods():
            page.text = "Choose how to verify"
            page.controls = [Control(page, "Use authenticator app", on_click=show_authenticator)]

        page.controls = [Control(page, "Try another way", on_click=show_methods)]
        self.run_challenge(page)
        self.assertEqual(page.clicked, ["Try another way", "Use authenticator app", "Continue"])
        self.assertEqual(len(page.entered), 1)

    def test_sms_only_challenge_is_not_given_totp_or_triggered(self):
        page = LoginPage("We sent a text message to your phone")
        page.controls = [Control(page, "Text a code")]
        with self.assertRaisesRegex(AuthenticatorError, "TOTP_METHOD_UNAVAILABLE"):
            self.run_challenge(page)
        self.assertEqual(page.clicked, [])
        self.assertEqual(page.entered, [])

    def test_observed_intuit_nested_method_menus(self):
        page = LoginPage("Check your text messages")
        def show_authenticator():
            page.text = "Enter the code from your authenticator app"
            page.controls = [Control(page, "Continue")]
        def show_more():
            page.text = "Verify it's you"
            page.controls = [Control(page, "Use authenticator app", on_click=show_authenticator)]
        def show_methods():
            page.text = "Verify it's you"
            page.controls = [Control(page, "Text a code"), Control(page, "Call with a code"),
                             Control(page, "See more options", on_click=show_more)]
        page.controls = [Control(page, "I didn't get a text message", on_click=show_methods)]
        self.run_challenge(page)
        self.assertEqual(page.clicked, ["I didn't get a text message", "See more options",
                                        "Use authenticator app", "Continue"])
        self.assertEqual(len(page.entered), 1)

    def test_observed_unenrolled_account_stops_without_identity_recovery(self):
        page = LoginPage("Check your text messages")
        def show_more():
            page.text = "Verify it's you"
            page.controls = [Control(page, "Text a code"), Control(page, "Call with a code"),
                             Control(page, "Verify identity a different way (takes longer)")]
        def show_methods():
            page.text = "Verify it's you"
            page.controls = [Control(page, "See more options", on_click=show_more)]
        page.controls = [Control(page, "I didn't get a text message", on_click=show_methods)]
        with self.assertRaisesRegex(AuthenticatorError, "TOTP_METHOD_UNAVAILABLE"):
            self.run_challenge(page)
        self.assertEqual(page.clicked, ["I didn't get a text message", "See more options"])
        self.assertEqual(page.entered, [])

    def test_foreign_origin_and_account_enrollment_are_rejected(self):
        for url in ("https://accounts.intuit.com.attacker.example/app/sign-in",
                    "http://accounts.intuit.com/app/sign-in",
                    "https://qbo.intuit.com.attacker.example/app/homepage",
                    "https://accounts.intuit.com/app/account-manager/security/authenticator"):
            with self.subTest(url=url):
                page = LoginPage()
                page.url = url
                with self.assertRaisesRegex(AuthenticatorError, "TOTP_UNEXPECTED_LOGIN_ORIGIN"):
                    self.run_challenge(page)
                self.assertEqual(page.entered, [])

    def test_redirect_during_method_selection_is_rejected(self):
        page = LoginPage("Choose how to verify")
        def redirect():
            page.url = "https://bank.example/login"
        page.controls = [Control(page, "Authenticator app", on_click=redirect)]
        with self.assertRaisesRegex(AuthenticatorError, "TOTP_UNEXPECTED_LOGIN_ORIGIN"):
            self.run_challenge(page)
        self.assertEqual(page.entered, [])

    def test_expiring_code_waits_for_next_step(self):
        page = LoginPage()
        now = [59]
        waits = []
        def sleep(seconds):
            waits.append(seconds)
            now[0] += seconds
        self.run_challenge(page, clock=lambda: now[0], sleep=sleep)
        self.assertEqual(waits, [1.5])
        self.assertEqual(page.entered, [generate_totp(RFC_KEY, 60.5)])

    def test_rejected_code_stops_without_sms_or_retries(self):
        page = LoginPage()
        page.reject = True
        with self.assertRaisesRegex(AuthenticatorError, "TOTP_VERIFICATION_FAILED"):
            self.run_challenge(page)
        self.assertEqual(len(page.entered), 1)
        self.assertEqual(page.clicked, ["Continue"])

    def test_browser_errors_do_not_expose_the_code(self):
        page = LoginPage()
        code = generate_totp(RFC_KEY, 45)
        page.fields[0].fill = unittest.mock.Mock(side_effect=RuntimeError("Could not fill " + code))
        with self.assertRaisesRegex(AuthenticatorError, "TOTP_SUBMISSION_FAILED") as ctx:
            self.run_challenge(page)
        self.assertNotIn(code, str(ctx.exception))
        self.assertTrue(ctx.exception.__suppress_context__)

    def test_auto_submitting_form_does_not_require_a_continue_button(self):
        page = LoginPage()
        page.controls = []
        def fill(value):
            page.entered.append(value)
            page.url = "https://qbo.intuit.com/app/homepage"
        page.fields[0].fill = fill
        self.run_challenge(page)
        self.assertEqual(len(page.entered), 1)

    def test_missing_input_or_submit_fails_explicitly(self):
        page = LoginPage()
        page.fields = []
        with self.assertRaisesRegex(AuthenticatorError, "TOTP_CODE_INPUT_UNSUPPORTED"):
            self.run_challenge(page)
        self.assertEqual(page.entered, [])
        page = LoginPage()
        page.controls = []
        with self.assertRaisesRegex(AuthenticatorError, "TOTP_SUBMIT_UNAVAILABLE"):
            self.run_challenge(page)

    def test_captcha_requires_intervention_without_code_submission(self):
        page = LoginPage("Please complete the CAPTCHA")
        with self.assertRaisesRegex(AuthenticatorError, "TOTP_INTERACTIVE_CHALLENGE_REQUIRED"):
            self.run_challenge(page)
        self.assertEqual(page.entered, [])

    def test_already_signed_in_does_not_submit_code(self):
        page = LoginPage()
        page.url = "https://qbo.intuit.com/app/homepage"
        self.run_challenge(page)
        self.assertEqual(page.entered, [])


class AppIntegrationTests(unittest.TestCase):
    def test_both_login_paths_use_authenticator_when_configured(self):
        # Exercise the real app entry points through their login/challenge branches.
        import app

        class Element:
            def click(self):
                pass

        class Page:
            url = "https://accounts.intuit.com/app/sign-in"
            keyboard = unittest.mock.Mock()
            mouse = unittest.mock.Mock()
            def goto(self, *args, **kwargs):
                pass
            def query_selector(self, selector):
                return Element()
            def wait_for_selector(self, *args, **kwargs):
                return Element()
            def wait_for_url(self, *args, **kwargs):
                raise TimeoutError("Challenge required")

        for entrypoint in (app.auto_login, app.auto_login_and_refresh):
            with self.subTest(entrypoint=entrypoint.__name__):
                manager = unittest.mock.MagicMock()
                context = manager.__enter__.return_value.chromium.launch.return_value.new_context.return_value
                page = Page()
                context.new_page.return_value = page
                # Stop immediately at dispatch, before bank access, refresh, or writes.
                with patch.object(app, "sync_playwright", return_value=manager), \
                        patch.object(app, "QB_USERNAME", "test@example.invalid"), \
                        patch.object(app, "QB_PASSWORD", "offline-test-value"), \
                        patch.object(app, "human_delay"), patch.object(app.time, "sleep"), \
                        patch.dict(app.os.environ, {"QB_TOTP_SECRET": RFC_KEY}), \
                        patch.object(app, "complete_totp_challenge", side_effect=AuthenticatorError("TEST_STOP")) as dispatch:
                    with self.assertRaisesRegex(AuthenticatorError, "TEST_STOP"):
                        entrypoint()
                    dispatch.assert_called_once_with(page, RFC_KEY)


if __name__ == "__main__":
    unittest.main()
