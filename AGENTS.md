# AGENTS.md

This repo is `pdt-meltano`, a Meltano utility built with the Meltano EDK. It deploys the job schedules of a Meltano project through `pdt-cli`. It holds no cloud code: every cloud action is a `pdt` command run on the pdt project it writes into `.meltano/run/pdt/`. A cloud feature starts in pdt, not here.

- The three `.yml` files in `hub/` are the Meltano Hub entries. `hub/autoidm.png` is the logo that each entry names in `logo_url`. The entries differ only in `name`, `namespace`, `label`, and the default `provider` and `region` values. Keep them the same in every other key.
- `pdt_meltano/extension.py` reads the schedules with `meltano schedule list --format=json` and writes the pdt project. `RUN_PY` and `DOCKERFILE` are the files every app gets.
- Tests need no network, no Meltano install, and no cloud account.
- Write each Markdown paragraph as one line.
