# pdt-meltano

pdt-meltano is a Meltano utility that wraps `pdt`. It runs your Meltano job schedules in your own cloud account. Each job schedule in `meltano.yml` becomes a scheduled container job. [AutoIDM](https://www.autoidm.com/) makes pdt-meltano and [pdt](https://github.com/AutoIDM/pdt).

## Why use pdt-meltano

Because it's alarmingly easy and lightweight. Deploy to your existing infrastructure right from your meltano project. You own everything and there's nothing extra to set up: no SAAS / PAAS, no subscription fees, no daemon. Works with your existing project and plays nicely with AI tools.

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
meltano schedule add daily-sync --job github-to-postgres --interval '@daily'
```

Then deploy:

```
meltano invoke pdt-aws deploy
```

`deploy` shows you a plan and a monthly cost estimate, and it asks before it changes anything. The first time, it also asks you to sign in to your cloud account. Every value in your project's `.env` goes to the cloud job as a secret, so when you change a value, deploy again.

A schedule with the interval `@manual` or `@once` does not repeat, so pdt-meltano leaves it out and tells you why. So does a schedule that runs `meltano elt` instead of a job.

## Commands

Commands take schedule names, the same names that `meltano schedule list` shows. For `deploy` and `destroy`, the schedule names are optional; leave them out to act on every schedule.


| Command                                | What it does                                            |
| -------------------------------------- | ------------------------------------------------------- |
| `deploy [<schedule-name>...] [--yes]`  | deploy each schedule, or only the ones you name         |
| `destroy [<schedule-name>...] [--yes]` | remove everything that deploy made in the cloud         |
| `<pdt-command>...`                     | run any other pdt command, such as `list` or `validate` |
| `--help`                               | list all pdt commands                                   |
| `describe [--format <format>]`         | list the commands of pdt-meltano                        |
| `initialize`                           | does nothing, because deploy writes what it needs       |


`--yes` skips the question that `deploy` and `destroy` ask before they change anything in the cloud. The `<format>` of `describe` is `text`, `json`, or `yaml`.

## Config

pdt-meltano writes a pdt project into `.meltano/run/pdt/` each time you run a command. Set them with Meltano, not in `pdt.yml`. Every setting you set with `meltano config set pdt-aws <setting> <value>` goes under `platform:` in `pdt.yml` with the same name. Run `meltano invoke pdt-aws validate` to see a problem before you deploy.

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

pdt-meltano reads your schedules from `meltano.yml` each time you run a command, so there is nothing to sync. Change a schedule's interval or job, then run `deploy` to send the change to the cloud.

If you remove a schedule that is still deployed, its cloud job keeps running. pdt-meltano reminds you on each command until you run `destroy <schedule-name>`.

## Working with your team

pdt-meltano treats each schedule in meltano.yml as an app. A cloned meltano project that uses pdt-meltano manages the same jobs. This is intended: when you and a teammate each deploy `daily-sync` from your own copy, you both update the same job.

Two different Meltano projects that deploy to the same cloud account must not use the same schedule name. If they do, each deploy replaces the other project's job.

## License

MIT. See `LICENSE`.