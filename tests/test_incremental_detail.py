"""Execute the real incremental extraction script against minimal public DOMs."""

import json
import shutil
import subprocess

import pytest

from src.refresh_transport import RefreshTransport
from tests.test_active_refresh import card, execute, rows, setup as _setup
from tests.test_refresh_transport import Session, proof

setup = _setup


def extracted(script, heading):
    node = shutil.which("node")
    assert node, "Node is required to verify the real detail extraction script"
    result = subprocess.run(
        [
            node,
            "-e",
            """
      const vm = require('node:vm');
      const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
      const document = {
        querySelector: () => null,
        querySelectorAll: selector => selector === 'script' ? [{textContent:
          'buyer_agent_full_name:"Example Agent",list_agent_full_name:"Example Agent",listing_id:"new",listing_contract_date:"2026-09-01",mls_status:"Active"'}]
          : selector === 'h1' && input.heading ? [{textContent: input.heading[0], nextElementSibling: {textContent: input.heading[1]}}] : []
      };
      process.stdout.write(vm.runInNewContext(input.script, {document}));
    """,
        ],
        input=json.dumps(dict(script=script, heading=heading)),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize("heading", [["12 Real Road", "York, ME 03909"], None])
def test_transport_executes_actual_visible_detail_recovery(tmp_path, setup, heading):
    session = Session()

    def post(*args, **kwargs):
        session.posts.append(kwargs)
        script = kwargs["json"]["actions"][-1]["script"]
        from tests.test_refresh_transport import Response

        return Response(
            {
                "success": True,
                "data": {
                    "rawHtml": "fixture DOM",
                    "metadata": {"statusCode": 200},
                    "actions": {
                        "javascriptReturns": [json.dumps(extracted(script, heading))]
                    },
                },
            }
        )

    session.post = post
    client = RefreshTransport(
        api_key="fixture-key",
        policy_path=proof(tmp_path),
        reserve=lambda *args: None,
        session=session,
    )
    result = execute(
        setup,
        [card("old"), card("gone"), dict(card("new"), address=None, city=None)],
        fetch_detail=client.detail,
    )
    if heading:
        assert "new" in result["active_mls_ids"]
        assert rows(setup)["new"]["address"] == heading[0]
        assert rows(setup)["new"]["city"] == "York"
    else:
        assert "new" not in result["active_mls_ids"]
        assert len(result["unresolved"]) == 1
