# AGENTS.md

This repo is `pdt-meltano`, a Meltano utility built with the Meltano EDK. It deploys the job schedules of a Meltano project through `pdt-cli`. It holds no cloud code: every cloud action is a `pdt` command run on the pdt project it writes into the run folder Meltano gives the plugin, `$MELTANO_SYS_DIR_ROOT/run/$MELTANO_UTILITY_NAME/`. A cloud feature starts in pdt, not here.

- The three `.yml` files in `hub/` are the Meltano Hub entries. Each entry names its own logo in `logo_url`: `hub/pdt-aws.png`, `hub/pdt-gcloud.png`, or `hub/pdt-azure.png`. Each logo is a 400 x 400 PNG with the platform logo on a white background. `hub/autoidm.png` is the AutoIDM logo. The entries differ only in `name`, `namespace`, `label`, `logo_url`, and the default `provider` and `region` values. Keep them the same in every other key.
- `pdt_meltano/extension.py` reads the schedules with `meltano schedule list --format=json` and writes the pdt project. `RUN_PY` and `DOCKERFILE` are the files every app gets.
- Tests need no network, no Meltano install, and no cloud account.
- Write each Markdown paragraph as one line.
