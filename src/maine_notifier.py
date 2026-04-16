"""Notification helpers for Maine Listings pipeline.

Sends Pushover (real-time) + Resend (email summary) alerts on failures,
circuit-breaker aborts, and completion summaries. Reads credentials from
~/.env (shared across all projects) or project .env.

Silent no-ops if keys are missing — the pipeline should never crash because
a notifier isn't configured.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import urllib.request
import urllib.parse

logger = logging.getLogger(__name__)

_DEFAULT_EMAIL_TO = 'lucas.knowles@grandwelcome.com'
_DEFAULT_EMAIL_FROM = 'IT@scmaine.com'


def _get_env(key: str) -> Optional[str]:
    """Return a secret from env. Loads ~/.env on first call if not already set."""
    val = os.environ.get(key)
    if val:
        return val
    # Lazy-load ~/.env (shared secrets file)
    home_env = os.path.expanduser('~/.env')
    if os.path.exists(home_env):
        with open(home_env) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, _, v = line.partition('=')
                if k.strip() == key:
                    val = v.strip().strip('"').strip("'")
                    os.environ[key] = val
                    return val
    return None


def send_pushover(
    title: str,
    message: str,
    *,
    priority: int = 0,
    url: Optional[str] = None,
) -> bool:
    """Send a Pushover notification. Priority: -2..2 (2 = emergency).

    Returns True on success, False on any failure (silently logged).
    """
    token = _get_env('PUSHOVER_API_TOKEN')
    user = _get_env('PUSHOVER_USER_KEY')
    if not token or not user:
        logger.debug('Pushover keys missing, skipping notification')
        return False

    data = {
        'token': token,
        'user': user,
        'title': title[:250],
        'message': message[:1024],
        'priority': priority,
    }
    if url:
        data['url'] = url

    try:
        req = urllib.request.Request(
            'https://api.pushover.net/1/messages.json',
            data=urllib.parse.urlencode(data).encode('utf-8'),
            headers={'User-Agent': 'maine-listings-pipeline/1.0'},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as exc:
        logger.warning('Pushover send failed: %s', exc)
        return False


def send_email(
    subject: str,
    body_text: str,
    *,
    body_html: Optional[str] = None,
    to_addr: Optional[str] = None,
) -> bool:
    """Send an email via Resend API. Returns True on success."""
    api_key = _get_env('RESEND_API_KEY')
    if not api_key:
        logger.debug('RESEND_API_KEY missing, skipping email')
        return False

    import json
    payload = {
        'from': _get_env('EMAIL_FROM') or _DEFAULT_EMAIL_FROM,
        'to': [to_addr or _DEFAULT_EMAIL_TO],
        'subject': subject,
        'text': body_text,
    }
    if body_html:
        payload['html'] = body_html

    try:
        req = urllib.request.Request(
            'https://api.resend.com/emails',
            data=json.dumps(payload).encode('utf-8'),
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json',
                'User-Agent': 'maine-listings-pipeline/1.0',
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status in (200, 202)
    except Exception as exc:
        logger.warning('Resend email failed: %s', exc)
        return False


def notify_failure(context: str, error: str, *, run_id: Optional[str] = None) -> None:
    """High-priority alert: pipeline hit a failure.

    Fires Pushover (priority 1 = bypasses quiet hours) + Resend email.
    """
    title = 'Maine Listings: pipeline failure'
    run_tag = f' [{run_id}]' if run_id else ''
    message = f'{context}{run_tag}\n\nError: {error[:800]}'
    send_pushover(title, message, priority=1)
    send_email(
        subject=f'[Maine Listings] FAILURE: {context}',
        body_text=message,
    )


def notify_success(summary: str, *, details: Optional[str] = None) -> None:
    """Normal-priority alert: run completed."""
    title = 'Maine Listings: run complete'
    body = summary + (f'\n\n{details}' if details else '')
    send_pushover(title, body[:1024], priority=0)
    send_email(
        subject=f'[Maine Listings] {summary}',
        body_text=body,
    )
