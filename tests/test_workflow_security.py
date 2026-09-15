import re
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = (
    REPO_ROOT / ".github/workflows/maine_listings.yml",
    REPO_ROOT / ".github/workflows/scrape_agents.yml",
    REPO_ROOT / ".github/workflows/zillow_leaderboard.yml",
)


def expressions_in_run_blocks(workflow: Path) -> list[tuple[int, str]]:
    findings: list[tuple[int, str]] = []
    run_indent: int | None = None

    for line_number, line in enumerate(workflow.read_text().splitlines(), start=1):
        match = re.match(r"^(\s*)(?:-\s+)?run:\s*(.*)$", line)
        if match:
            run_value = match.group(2).strip()
            if run_value.startswith(("|", ">")):
                run_indent = len(match.group(1))
            else:
                run_indent = None
                if "${{" in run_value:
                    findings.append((line_number, run_value))
            continue

        if run_indent is None:
            continue

        stripped = line.strip()
        indent = len(line) - len(line.lstrip())
        if stripped and indent <= run_indent:
            run_indent = None
            continue

        if "${{" in line:
            findings.append((line_number, stripped))

    return findings


class WorkflowSecurityTests(unittest.TestCase):
    def test_scanner_covers_inline_and_folded_shell_steps(self) -> None:
        cases = {
            "inline.yml": 'steps:\n  - run: echo "${{ github.event.inputs.mode }}"\n',
            "folded.yml": 'steps:\n  - run: >\n      echo "${{ github.event.inputs.mode }}"\n',
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            for name, content in cases.items():
                workflow = Path(temp_dir) / name
                workflow.write_text(content)
                with self.subTest(name=name):
                    self.assertTrue(expressions_in_run_blocks(workflow))

    def test_dispatch_inputs_are_not_spliced_into_shell_scripts(self) -> None:
        failures = {
            workflow.name: expressions_in_run_blocks(workflow)
            for workflow in WORKFLOWS
        }
        failures = {name: findings for name, findings in failures.items() if findings}
        self.assertEqual({}, failures)


if __name__ == "__main__":
    unittest.main()
