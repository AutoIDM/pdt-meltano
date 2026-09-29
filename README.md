# pdt-meltano

A Meltano utility that deploys each job schedule of a Meltano project as a scheduled container job in your cloud account, using [pdt](https://github.com/AutoIDM/pdt). [AutoIDM](https://www.autoidm.com/) makes pdt and pdt-meltano. It has three Meltano Hub entries, one for each cloud:

| Hub entry | Where the jobs run |
| --- | --- |
| `pdt-aws` | AWS Batch on Fargate |
| `pdt-azure` | Azure Container Apps Jobs |
| `pdt-gcloud` | Google Cloud Run Jobs |

## Use

```
meltano add utility pdt-azure
meltano config set pdt-azure region eastus2
meltano invoke pdt-azure deploy
```

`deploy` makes one cloud job for each job schedule (`meltano schedule list`). It shows a plan and a monthly cost estimate, and asks before it changes anything. The first deploy asks you to sign in to your cloud account. Name schedules after `deploy` to deploy only those. `--yes` skips the question.

| Command | What it does |
| --- | --- |
| `deploy [SCHEDULE...]` | deploy each job schedule, or only the ones named |
| `destroy [SCHEDULE...]` | remove everything deploy created |
| `runs SCHEDULE` | list the recent runs of one schedule |
| `logs SCHEDULE [N]` | read the log of run N (1 is the newest) |
| `health` | show whether the last run of each schedule succeeded |
| `invoke PDT-ARGS...` | run any pdt command on the project pdt-meltano writes |

Only job schedules with a repeating interval deploy. A schedule of `meltano elt` or one set to `@manual` or `@once` is left out, and deploy says why.

## What happens

pdt deploys an app: a folder holding `run.py` next to a `pdt.yml`. pdt-meltano writes one app for each schedule into `.meltano/run/pdt/`. Each app folder is a copy of your Meltano project without `.meltano/`, `.env`, and `.git`, plus a generated `run.py`, `config.yml`, and `Dockerfile`. The image installs the plugins of that schedule when it is built (`meltano install --schedule`), with the Meltano version you use now.

A cloud job starts from a new container each time. So `run.py` keeps the Meltano system database, which holds each extractor's state, in the app's pdt storage folder in your cloud account. It copies the database down before `meltano run` and back up after, and a lock stops two runs of the same schedule from using it at the same time.

Every name in your project's `.env` goes to the cloud job as a secret. Change a value, then deploy again.

## Develop

```
uv sync
uv run pytest
```

The `.yml` files in `hub/` are the three Meltano Hub entries, and `hub/autoidm.png` is their logo; a Hub pull request copies it to `static/assets/logos/utilities/autoidm.png`. To try one in a Meltano project before it is on the Hub, run `meltano add utility pdt-aws --from-ref <path to hub/pdt-aws.yml>`, then set `pip_url` in `meltano.yml` to `-e <path to this folder>` and run `meltano install`.

## License

MIT. See `LICENSE`.
