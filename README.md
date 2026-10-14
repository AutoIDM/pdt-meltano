# pdt-meltano

pdt-meltano is a Meltano utility that wraps `pdt`. It runs your Meltano job schedules in your own cloud account. Each job schedule in `meltano.yml` becomes a scheduled container job. [AutoIDM](https://www.autoidm.com/) makes pdt-meltano and [pdt](https://github.com/AutoIDM/pdt).

## Why use pdt-meltano

Because it's easy and lightweight. Deploy to your existing infrastructure right from your meltano project. You own everything and there's nothing extra to set up: no SAAS / PAAS, no subscription fees, no daemon. Works with your existing project and plays nicely with AI tools.

## Pick your cloud

There is one Meltano Hub entry for each cloud. Add the one for the cloud you use:


| Hub entry                                      | Where the jobs run        | Default region |
| ---------------------------------------------- | ------------------------- | -------------- |
| `meltano add --plugin-type utility pdt-aws`    | AWS Batch on Fargate      | `us-east-1`    |
| `meltano add --plugin-type utility pdt-azure`  | Azure Container Apps Jobs | `eastus2`      |
| `meltano add --plugin-type utility pdt-gcloud` | Google Cloud Run Jobs     | `us-central1`  |


The three entries have the same commands, and each command works the same way on each cloud. The examples below use `pdt-aws`. For another cloud, put `pdt-azure` or `pdt-gcloud` in its place.

## Deploy your first schedule

Add the utility, and set the region if you do not want the default:

```
meltano add --plugin-type utility pdt-aws
meltano config set pdt-aws region us-west-2
```

pdt-meltano deploys job schedules only, so you need a job and a schedule for it. If you have them already, skip this step:

```
meltano job add github-to-postgres --tasks "tap-github target-postgres"
meltano schedule add meltano-daily-sync --job github-to-postgres --interval '@daily'
```

Then deploy:

```
meltano invoke pdt-aws deploy
```

`deploy` shows you a plan and a monthly cost estimate, and it asks before it changes anything. The first time, it also asks you to sign in to your cloud account. Every value in your project's `.env` goes to the cloud job as a secret, so when you change a value, deploy again.

A schedule with the interval `@manual` or `@once` does not repeat, so pdt-meltano leaves it out and tells you why. So does a schedule that runs `meltano elt` instead of a job.

## Commands

Every command takes schedule names, the same names that `meltano schedule list` shows. This includes the pdt commands that act on one schedule, such as `logs`, `runs`, `pause`, `unpause`, `run --deployed`, `secrets`, and `storage`. pdt-meltano gives pdt the job name of that schedule in the active Meltano environment. For `deploy` and `destroy`, the schedule names are optional; leave them out to act on every schedule.

Each Meltano environment deploys its own job for each schedule. The job name is the schedule name and the environment name, for example `meltano-daily-sync-prod` for the schedule `meltano-daily-sync` in `prod`. Choose the environment with `--environment`, for example `meltano --environment prod invoke pdt-aws logs meltano-daily-sync`. Without `--environment`, the command uses `default_environment` from `meltano.yml`. pdt's own output and hints name the job, for example `pdt logs meltano-daily-sync-prod`. Type the schedule name in its place, or the job name, which pdt-meltano also accepts.


| Command                                | What it does                                            |
| -------------------------------------- | ------------------------------------------------------- |
| `deploy [<schedule-name>...] [--yes]`  | deploy each schedule, or only the ones you name         |
| `destroy [<schedule-name>...] [--yes]` | remove everything that deploy made in the cloud         |
| `<pdt-command>...`                     | run any other pdt command, such as `list` or `validate` |
| `--help`                               | list all pdt commands                                   |
| `describe [--format <format>]`         | list the commands of pdt-meltano                        |
| `initialize`                           | write a starter shared `pdt.yml` if none exists         |


`--yes` skips the question that `deploy` and `destroy` ask before they change anything in the cloud. The `<format>` of `describe` is `text`, `json`, or `yaml`.

## Config

pdt-meltano writes a pdt project for the active Meltano environment into `.meltano/utilities/pdt-aws/pdt/<environment>/` each time you run a command. `meltano install` keeps this folder, so the list of deployed schedules and the values pdt saves on your first deploy stay. Set the plugin settings with Meltano, not in `pdt.yml`. Each setting in the table below that you set with `meltano config set pdt-aws <setting> <value>` goes under `platform:` in `pdt.yml` with the same name. pdt-meltano puts no other setting under `platform:`, so an env var such as `PDT_AWS_OTHER` in your shell does not change it. Run `meltano invoke pdt-aws validate` to see a problem before you deploy.

These are the settings that pdt reads:


| Setting          | Cloud        | What it sets                                                                |
| ---------------- | ------------ | --------------------------------------------------------------------------- |
| `provider`       | all          | the cloud; the Hub entry sets it, so do not change it                       |
| `region`         | all          | where the jobs run                                                          |
| `timezone`       | all          | the time zone of the schedule intervals; Azure accepts only `Etc/UTC`       |
| `account`        | AWS          | the AWS account; pdt adds it on your first deploy                           |
| `profile`        | AWS          | the profile in `~/.aws` to use; pdt adds it if it has to ask you            |
| `subscription`   | Azure        | the Azure subscription; pdt adds it on your first deploy                    |
| `resource_group` | Azure        | the resource group for the jobs                                             |
| `environment`    | Azure        | a Container Apps environment you already have, as `<resource-group>/<name>` |
| `project`        | Google Cloud | the Google Cloud project; pdt adds it on your first deploy                  |


Secrets come from your project's `.env`, as described in [Deploy your first schedule](#deploy-your-first-schedule).

Settings that are the same for every cloud, such as `alerts:`, live in `orchestrate/pdt/pdt.yml`. Every pdt plugin of the project reads this file. Commit it. The `config_dir` setting names its folder, and the default is `$MELTANO_PROJECT_ROOT/orchestrate/pdt`. `config_dir` does not go under `platform:`.

`meltano invoke pdt-aws initialize` writes a starter file with a commented `alerts:` example. It does not replace a file that exists. `meltano invoke pdt-aws alerts setup` writes `alerts:` into the file and keeps your comments. It makes the folder and the file first if they do not exist. pdt-meltano then moves the webhook from `orchestrate/pdt/.env` to the `.env` of your project.

Every Meltano environment uses the same folder. To give one environment its own file, set `config_dir` for that environment in `meltano.yml`. Each pdt plugin has its own `config_dir` setting, so the override names each plugin:

```yaml
environments:
- name: dev            # no entry: every pdt plugin reads orchestrate/pdt/pdt.yml
- name: prod
  config:
    plugins:
      utilities:
      - name: pdt-aws
        config:
          config_dir: $MELTANO_PROJECT_ROOT/orchestrate/pdt-prod
      - name: pdt-azure
        config:
          config_dir: $MELTANO_PROJECT_ROOT/orchestrate/pdt-prod
```

An `env:` value for the environment does not change `config_dir`. Meltano reads the setting from `config:`.

pdt-meltano reads your schedules from `meltano.yml` each time you run a command, so there is nothing to sync. Change a schedule's interval or job, then run `deploy` to send the change to the cloud.

If you remove a schedule that is still deployed, its cloud job keeps running. pdt-meltano reminds you on each command until you run `destroy <schedule-name>`.

## Working with your team

pdt-meltano deploys each schedule in meltano.yml once for each Meltano environment, as the job `<schedule>-<environment>`. A cloned meltano project that uses pdt-meltano manages the same jobs. This is intended: when you and a teammate each deploy `meltano-daily-sync` in `prod` from your own copy, you both update the job `meltano-daily-sync-prod`. A deploy in `dev` updates `meltano-daily-sync-dev` and leaves the `prod` job and its extractor state alone.

Two different Meltano projects that deploy to the same cloud account must not use the same schedule name in the same environment. If they do, each deploy replaces the other project's job.

An `apps:` entry in the shared `pdt.yml` names the job, for example `meltano-daily-sync-prod`, so it can set alerts for one environment.

## Install from GitHub

Until the Hub entries are published, add the plugin to `meltano.yml` yourself. This `pip_url` installs pdt-meltano from GitHub:

```yaml
plugins:
  utilities:
  - name: pdt-aws
    namespace: pdt_aws
    pip_url: git+https://github.com/AutoIDM/pdt-meltano.git
    executable: pdt_meltano
    settings:
    - name: provider
      value: aws
    - name: region
      value: us-east-1
```

Then install it:

```
meltano install utility pdt-aws
```

For another cloud, change `name`, `namespace`, and the two setting values to the ones in the matching file in `hub/`:


| `name`       | `namespace`  | `provider` | `region`      |
| ------------ | ------------ | ---------- | ------------- |
| `pdt-aws`    | `pdt_aws`    | `aws`      | `us-east-1`   |
| `pdt-azure`  | `pdt_azure`  | `azure`    | `eastus2`     |
| `pdt-gcloud` | `pdt_gcloud` | `gcloud`   | `us-central1` |


To use a local copy of this repo instead, set `pip_url` to `-e /path/to/pdt-meltano`.

## License

MIT. See `LICENSE`.
