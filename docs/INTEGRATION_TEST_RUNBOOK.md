# INTEGRATION_TEST_RUNBOOK.md — real-XNAT round-trip with synthetic data

**Status:** planned, not yet executed. **Why this exists:** to validate how the
*original* `src/` code actually behaves against a **real** XNAT server (not the
FakeXNAT double) by pushing + pulling **synthetic, no-PHI** data. Containers here
are ephemeral, so this runbook lets a fresh session pick up the plan immediately.

## Why it wasn't run on 2026-06-05

The web sandbox's network policy **blocked Docker Hub's blob CDN**
(`production.cloudfront.docker.com` → 403) so no image could be pulled, and
`central.xnat.org` was also 403'd. `xnat4tests` (the right tool) needs to pull a
Tomcat base image + XNAT artifacts. **Fix: relaunch in an environment whose network
policy allows Docker Hub egress** (see code.claude.com/docs/en/claude-code-on-the-web,
network-policy section). Then run this runbook.

## Required network egress (if using a custom allowlist instead of full/open)

- `registry-1.docker.io`, `auth.docker.io`, `production.cloudfront.docker.com` (Docker Hub)
- XNAT artifact hosts used during the xnat4tests image build (XNAT WAR + plugins):
  `*.xnat.org`, `bitbucket.org`, `github.com`, `objects.githubusercontent.com`
- `pypi.org` / `files.pythonhosted.org` (pip)

Simplest: pick the most permissive ("open"/full egress) policy for this test.

## Tooling (free, local, no PHI)

- **xnat4tests** (`pip install xnat4tests`) — boots a real XNAT in one Docker
  container, purpose-built for tests. CLI: `xnat4tests start|stop|restart|add-data`.
- **pydicom** — generate synthetic DICOMs (no real patient data).
- Docker daemon must be running: `sudo dockerd >/tmp/dockerd.log 2>&1 &` if no socket.

## Connection facts (our tool)

- Config: `src/services/config.py` — env > file > default. Override for the test:
  `export XNAT_SERVER_URL=http://localhost:8080` and
  `export XNAT_PROJECT_NAME=<test_project>`.
- Default (do NOT use for the test): `https://rpacs.iibi.uiowa.edu/xnat/`,
  project `GROK_AHRQ_Data` (production — never push synthetic junk there).
- Credentials are interactive (`pwinput`) — for the test, drive the code with the
  xnat4tests admin creds (default `admin`/`admin`) via the gateway/`XNATConnection`
  directly in a script, not the interactive prompt.
- Entry points: `main.py`, `src/initialize_basic_metatable_items.py`.
- Real publish path: `src/xnat_experiment_data.py` `publish_to_xnat` →
  `src/utilities.py` `XNATConnection` + `ConfigTables`. Real pull: `app/logic/download.py`.

## Steps (execute on relaunch)

1. Start daemon (if needed) + `pip install xnat4tests`.
2. `xnat4tests start` (first run pulls images + boots Tomcat — allow ~5-10 min).
   Verify: `docker ps` shows the xnat4tests container; `curl -s -o /dev/null -w '%{http_code}' http://localhost:8080` → 200/302.
3. Create the test project + register the connecting user as project member
   (XNATConnection validates membership). Use xnatpy/pyxnat admin or the XNAT REST API.
4. Generate synthetic data: a small synthetic DICOM series (pydicom) mimicking an
   RF/fluoroscopy still — random pixel array, fake (clearly non-PHI) tags, valid
   SOPClassUID. Also a synthetic intake-form row.
5. **Push (real code):** drive `SourceRFSession` / `publish_to_xnat` against
   `http://localhost:8080`. Capture every failure verbatim.
6. **Verify on server:** confirm subject/experiment/scan/resource/files exist via
   REST; confirm ConfigTables JSON landed on the project `config` resource.
7. **Pull (real code):** run the download path; confirm files come back + decode.
   Expect the #25 gap to surface (synthesized filename vs real enumeration).
8. **Report:** what worked, what broke, exact errors, with file:line. This is the
   "how well does the original code actually work" answer.
9. `xnat4tests stop` to tear down.

## Safety

- Synthetic data only — random pixels + obviously-fake tags. No real PHI ever.
- Target `localhost` only — never the UIowa production server for this test.
- No credentials committed; admin creds are the local throwaway xnat4tests defaults.
