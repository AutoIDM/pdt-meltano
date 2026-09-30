# pdt-meltano

pdt-meltano is a Meltano utility that runs your Meltano job schedules in your own cloud account. Each job schedule in `meltano.yml` becomes a scheduled container job, and you manage it by its schedule name. [AutoIDM](https://www.autoidm.com/) makes pdt-meltano and [pdt](https://github.com/AutoIDM/pdt), the tool that does the cloud work.

## Why use pdt-meltano

Because it's alarmingly easy and lightweight. Deploy to your existing infrastructure right from your meltano project, you own everything. Nothing extra to set up: no SAAS / PAAS, no subscription fees, no daemon. Works with your existing project and plays nicely with AI tools.

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


| Command                        | What it does                                         |
| ------------------------------ | ---------------------------------------------------- |
| `deploy [<schedule-name>...]`  | deploy each schedule, or only the ones you name      |
| `destroy [<schedule-name>...]` | remove everything that deploy made in the cloud      |
| `invoke <pdt-command>...`      | run any other pdt command, for example `invoke list` |


## Config

pdt-meltano has two settings, `provider` and `region`. You set them with `meltano config`, the same way as for any other plugin, and Meltano keeps them in `meltano.yml`.

| Setting    | How you set it                                | Where it goes in `pdt.yml` |
| ---------- | --------------------------------------------- | -------------------------- |
| `provider` | the Hub entry sets it; do not change it       | `platform.provider`        |
| `region`   | `meltano config set pdt-aws region us-west-2` | `platform.region`          |

Those two are the only settings. Meltano lets you set any name, so `meltano config set pdt-aws timezone UTC` works, but pdt-meltano does not read it.

Each time you run a command, pdt-meltano writes a pdt project into `.meltano/run/pdt/`. Its `pdt.yml` looks like this:

```yaml
platform:
  provider: aws            # from the provider setting
  region: us-west-2        # from the region setting
  account: '123456789012'  # pdt adds this on your first deploy, and pdt-meltano keeps it
```

Do not edit `pdt.yml` yourself. `.meltano/` is not in Git, so your edits stay on your computer only.

Each schedule's interval comes from `meltano.yml`, and its secrets come from `.env`, as described in [Deploy your first schedule](#deploy-your-first-schedule).

## When you change meltano.yml

pdt-meltano reads your schedules from `meltano.yml` each time you run a command, so there is nothing to sync. Change a schedule's interval or job, then run `deploy` to send the change to the cloud.

If you remove a schedule that is still deployed, its cloud job keeps running. pdt-meltano reminds you on each command until you run `destroy <schedule-name>`.

## License

MIT. See `LICENSE`.