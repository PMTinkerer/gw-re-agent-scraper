"""Tests for the tabbed static index page generator."""
from __future__ import annotations

from pathlib import Path

from src.index_page import generate_index_html


def test_index_uses_generated_dashboard_artifacts(tmp_path):
    """Local data/index.html should iframe the files generated into data/."""
    (tmp_path / 'maine_dashboard.html').write_text('', encoding='utf-8')
    (tmp_path / 'zillow_directory_dashboard.html').write_text('', encoding='utf-8')
    (tmp_path / 'dashboard.html').write_text('', encoding='utf-8')

    output = generate_index_html(output_path=str(tmp_path / 'index.html'))
    content = Path(output).read_text(encoding='utf-8')

    assert 'id="maine" class="active" src="maine_dashboard.html"' in content
    assert 'id="zillow" src="zillow_directory_dashboard.html"' in content
    assert 'id="redfin" src="dashboard.html"' in content


def test_index_falls_back_to_legacy_zillow_dashboard(tmp_path):
    """Older Zillow-only runs should still be iframe-compatible."""
    (tmp_path / 'zillow_dashboard.html').write_text('', encoding='utf-8')

    output = generate_index_html(output_path=str(tmp_path / 'index.html'))
    content = Path(output).read_text(encoding='utf-8')

    assert 'id="zillow" src="zillow_dashboard.html"' in content


def test_tab_switching_deactivates_master_panel(tmp_path):
    """Leaving Leaderboard should not leave its absolute panel over the iframe tabs."""
    output = generate_index_html(output_path=str(tmp_path / 'index.html'))
    content = Path(output).read_text(encoding='utf-8')

    assert (
        'document.querySelectorAll(".tab-content iframe, .tab-content .master-tab")'
        in content
    )
