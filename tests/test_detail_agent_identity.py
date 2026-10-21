"""Run the real detail script: one person's contact, or none when ambiguous."""
from __future__ import annotations

import json
import shutil
import subprocess

import pytest

from src.maine_parser import DETAIL_EXTRACT_JS, parse_detail_response
from tests.test_active_refresh import card, execute, rows
from tests.test_active_refresh import setup as _setup

setup = _setup

RUNNER = """
const vm = require('node:vm');
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const document = {
  querySelector: () => null,
  querySelectorAll: s => s === 'script' ? [{textContent: input.source}] : [],
};
const context = {document};
if (input.nuxt) context.window = {__NUXT__: input.nuxt};
process.stdout.write(vm.runInNewContext(input.script, context));
"""

FACTS = 'listing_id:"1700001",mls_status:"Active",listing_contract_date:"2026-10-01"'
PRIMARY = (
    'list_agent_full_name:"Jennifer Moore",list_agent_mls_id:"021771",'
    'list_agent_email:"jennifer.moore@remax.net"'
)


def run(source, nuxt=None):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required to run the real detail extraction script")
    result = subprocess.run(
        [node, "-e", RUNNER],
        input=json.dumps({"script": DETAIL_EXTRACT_JS, "source": source, "nuxt": nuxt}),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def blob(*parts):
    return "window.__NUXT__={" + ",".join(("buyer_agent_full_name:a", *parts)) + "}"


def test_single_listing_agent_is_taken_whole():
    data = run(blob("co_list_agent:{list_agent_full_name:a,list_agent_email:a}",
                    PRIMARY, FACTS))
    assert (data["listing_agent"], data["listing_agent_id"], data["listing_agent_email"]) == (
        "Jennifer Moore", "021771", "jennifer.moore@remax.net",
    )
    assert data["listing_agent_ambiguous"] is False
    assert data["error"] is None


def test_populated_co_agent_object_leaves_contact_empty():
    co = ('co_list_agent:{list_agent_full_name:"Kimberly Bohn",'
          'list_agent_mls_id:"020720",list_agent_email:"kim@example.com"}')
    data = run(blob(co, PRIMARY, FACTS))
    assert data["listing_agent_ambiguous"] is True
    assert data["listing_agent"] is None
    assert data["listing_agent_email"] is None
    assert data["error"] is None  # The listing itself is still usable.
    assert parse_detail_response(json.dumps(data))["mls_number"] == "1700001"


def test_co_agent_name_without_email_never_borrows_the_primary_email():
    co = 'co_list_agent:{list_agent_full_name:"Kimberly Bohn",list_agent_email:a}'
    data = run(blob(co, PRIMARY, FACTS))
    assert data["listing_agent_ambiguous"] is True
    assert data["listing_agent"] is None
    assert data["listing_agent_email"] is None


def test_prefixed_co_agent_keys_are_not_the_listing_agent():
    co = ('co_list_agent_full_name:"Kimberly Bohn",co_list_agent_mls_id:"020720",'
          'co_list_agent_email:"kim@example.com"')
    data = run(blob(co, PRIMARY, FACTS))
    assert data["listing_agent"] == "Jennifer Moore"
    assert data["listing_agent_email"] == "jennifer.moore@remax.net"
    assert data["listing_agent_ambiguous"] is False


def test_hoisted_primary_value_seen_through_evaluated_state():
    # The source shows the primary's email only as a variable; the quoted one
    # belongs to someone else. The evaluated state exposes both.
    source = blob('co_list_agent:{list_agent_email:"kim@example.com"}',
                  'list_agent_full_name:"Jennifer Moore",list_agent_email:b', FACTS)
    nuxt = {"data": [{
        "co_list_agent": {"list_agent_email": "kim@example.com"},
        "list_agent_full_name": "Jennifer Moore",
        "list_agent_email": "jennifer.moore@remax.net",
    }]}
    data = run(source, nuxt)
    assert data["listing_agent_ambiguous"] is True
    assert data["listing_agent_email"] is None


def test_same_person_repeated_with_escapes_and_case_is_not_ambiguous():
    source = blob(
        'list_agent_full_name:"Ann Lee",list_agent_email:"Ann.Lee@Example.com"',
        'list_office_name:"Better Homes\\u002FMasiello"', FACTS,
    )
    nuxt = {"x": {"list_agent_full_name": "Ann Lee",
                  "list_agent_email": "ann.lee@example.com"}}
    data = run(source, nuxt)
    assert data["listing_agent_ambiguous"] is False
    assert data["listing_agent"] == "Ann Lee"
    assert data["listing_agent_email"] == "ann.lee@example.com"
    assert parse_detail_response(json.dumps(data))["listing_office"] == (
        "Better Homes/Masiello"
    )


def test_ambiguous_buyer_agent_is_left_empty_too():
    data = run(blob(
        'buyer_agent_full_name:"Pat One",buyer_agent_email:"pat@example.com"',
        'co_buyer_agent:{buyer_agent_full_name:"Sam Two"}', PRIMARY, FACTS,
    ))
    assert data["buyer_agent_ambiguous"] is True
    assert data["buyer_agent"] is None
    assert data["listing_agent"] == "Jennifer Moore"


def test_ambiguous_new_listing_is_published_without_a_contact(setup):
    data = run(blob(
        'co_list_agent:{list_agent_full_name:"Kimberly Bohn",list_agent_email:a}',
        PRIMARY, 'listing_id:"new",mls_status:"Active",'
        'listing_contract_date:"2026-09-01"',
    ))
    result = execute(
        setup,
        [card("old"), card("gone"), card("new")],
        fetch_detail=lambda url: parse_detail_response(json.dumps(data)),
    )
    assert "new" in result["active_mls_ids"]
    row = rows(setup)["new"]
    assert row["listing_agent"] is None
    assert row["listing_agent_email"] is None
