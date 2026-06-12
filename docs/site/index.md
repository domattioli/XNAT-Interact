---
layout: default
title: XNAT-Interact — Getting Started
---

# XNAT-Interact

XNAT-Interact is a desktop tool for University of Iowa researchers who need to
de-identify and upload or download surgical fluoroscopic images to the UIowa
RPACS XNAT server. It runs on Windows and macOS, requires no programming
knowledge, and handles the de-identification and transfer steps for you.

> **This site stores no data.** It is a static informational page only — no
> forms, no logins, no analytics, no server-side processing.

---

## Before you start

You need three things in place before you open the app for the first time:

**① Get an XNAT account**
Register at the UIowa XNAT portal using your HawkID. Contact the **Data
Librarian** (your lab's designated point of contact) to get the registration
link and to be officially added to your research project. You cannot log in
until both steps are done.

**② Be added to the project**
Even after your account is created, the Data Librarian must add you to the
specific XNAT project your lab uses. If the app reports that you are not a
project member, this is the step to revisit.

**③ Connect to the UIowa VPN**
Whenever you use the app — on or off campus — you must be connected to the
[UIowa Cisco VPN](https://its.uiowa.edu/support/article/1876). The XNAT
server is not reachable without it.

---

## Get the app

Installation is handled by ITS, not by you directly. There are two paths:

**Primary — Software Center (recommended for most students)**
If you are on a UIowa-managed laptop or desktop, open the ITS Software Center
app (Windows) or Jamf Self Service (macOS), search for "XNAT-Interact", and
click Install. No admin password or terminal is required. Your machine
receives updates automatically through the same catalog.

**Fallback — signed self-served installer**
If your machine is not ITS-managed, ask the Data Librarian for the signed
installer link. Run it like any normal application installer — no admin
rights or terminal required.

---

## Heads up

- **SSL certificate renewals:** the XNAT server certificate is renewed
  annually. If the app reports an SSL error, contact the Data Librarian —
  you cannot connect until it is renewed.
- **Your password is never stored.** The app prompts for it each session and
  sends it only to the XNAT server.
- **De-identification happens on your machine.** Patient identifiers are
  removed before anything is uploaded.

---

## Who to contact

For access questions (account setup, project membership, installer links) and
for SSL certificate errors, contact your **Data Librarian** — the designated
point of contact for your lab or research group.

You can also [open an issue on GitHub](https://github.com/domattioli/XNAT-Interact/issues)
for software bugs or general questions.

---

## How we know it works

The upload/download pipeline is validated against a **live XNAT server** with
synthetic surgical fluoroscopy data — measured round-trips, stress lanes, and
duplicate-detection characterization, with every claim carrying its number.
Read the evidence: [Validated against a live XNAT server](validation.html).

---

*XNAT-Interact is a University of Iowa research tool. This site is hosted on
GitHub Pages and collects no user data.*
