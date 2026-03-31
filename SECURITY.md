# Security Policy

## Dependency Management

This project follows a strict supply chain security standard:

- **All Python dependencies are pinned to exact versions** (`==`) in `requirements.txt`
- **All GitHub Actions are pinned to full SHA** with version comments for auditability
- **Dependabot** is enabled for automated weekly dependency update PRs (pip + GitHub Actions)
- **pip-audit** runs in CI to detect known vulnerabilities before deployment

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly:

1. **Do not** open a public issue
2. Email **lucas.knowles@grandwelcome.com** with details
3. Include steps to reproduce if possible
4. Allow reasonable time for a fix before public disclosure

## Update Cadence

- Dependabot PRs are reviewed weekly
- Critical security patches are applied within 48 hours of disclosure
- Dependencies are audited on every CI run via pip-audit
