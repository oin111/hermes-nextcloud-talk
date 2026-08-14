# Security Policy

## Reporting a vulnerability

Please do not open a public issue for credential disclosure, authentication bypass, unsafe file handling, or unintended public file sharing.

Report the issue privately through GitHub's **Security → Report a vulnerability** feature for this repository. Include affected versions, reproduction steps, and impact when possible.

## Deployment notes

This plugin runs inside Hermes Gateway and uses a Nextcloud user's credentials. Operators should use a dedicated account, an app password, restrictive Talk room membership, and `NEXTCLOUD_TALK_ALLOWED_USERS`.
