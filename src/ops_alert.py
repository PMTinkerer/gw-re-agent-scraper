"""Send an operational alert to the Project Board (replaces failure emails).

Contract v1: one ``OPS_ALERT {json}`` line on stderr. The Project Board reads
GitHub Actions error/warning annotations of the latest run, so on Actions the
alert is also printed as a ``::error``/``::warning`` annotation. Never put
secrets, tokens, agent contact details or page text in title/detail.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

PREFIX = 'OPS_ALERT '
APP = 'gw-re-agent-scraper'
# GitHub Actions jobs: exit non-zero at the end if non-empty.
errors_fired: list[str] = []


def _gh_escape(s: str, prop: bool = False) -> str:
    s = s.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
    return s.replace(':', '%3A').replace(',', '%2C') if prop else s


def ops_alert(key: str, title: str, detail: str = '', *, severity: str = 'error',
              state: str = 'firing') -> None:
    rec = {'v': 1, 'app': os.getenv('OPS_ALERT_APP', APP), 'key': key[:80],
           'severity': severity, 'state': state, 'title': title[:200],
           'detail': detail[:500],
           'ts': datetime.now(timezone.utc).isoformat(timespec='seconds')}
    print(PREFIX + json.dumps(rec, separators=(',', ':')), file=sys.stderr,
          flush=True)
    if os.getenv('GITHUB_ACTIONS') == 'true' and state == 'firing':
        level = {'error': 'error', 'warn': 'warning'}.get(severity, 'notice')
        print(f'::{level} title={_gh_escape(title, True)}::'
              f'{_gh_escape(detail or title)}', flush=True)
    if severity == 'error' and state == 'firing':
        errors_fired.append(key)
