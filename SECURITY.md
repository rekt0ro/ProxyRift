# Security Policy

## Reporting a Vulnerability

Please report security vulnerabilities privately through [GitHub Private Vulnerability Reporting](https://github.com/rekt0ro/ProxyRift/security/advisories/new).

Include a description of the issue, its potential impact, the affected component, and steps to reproduce it. When relevant, include the affected commit SHA or GitHub Actions workflow run.

Please do not disclose exploitable vulnerabilities publicly before coordinating with the maintainer. Remove credentials, tokens, and other sensitive information from reports and logs.

## Scope

This policy covers security vulnerabilities in ProxyRift itself, including:

- Source discovery and the processing of externally sourced configuration data.
- Configuration parsing, validation, and subscription generation.
- The use and integrity of external validation tools and dependencies.
- GitHub Actions workflows, automated updates, repository permissions, and subscription publishing.
- Vulnerabilities that compromise the integrity of published subscription files or expose sensitive information.

## Validation Limitations

ProxyRift collects public proxy configurations and tests their compatibility, connectivity, and performance. The Light subscription uses stricter validation than All, but neither guarantees that a proxy server is trustworthy, private, or secure.

Third-party proxy failures, operator behavior, and server-side vulnerabilities are outside ProxyRift's direct control. However, vulnerabilities in ProxyRift that affect users or compromise the safety or integrity of its outputs remain in scope.
