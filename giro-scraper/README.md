# Giro d'Italia scraper

This directory now contains the Giro d'Italia route scraper and bundle artifacts.

Expected outputs:

- `giro_app_bundle.json`
- `giro_app_bundle_YYYY.json`
- `stages.csv`
- `classifications.csv`
- `teams.csv`
- `riders.csv`
- `stage_schedule.csv`
- `polling_plan.json`
- `suggested_cron.txt`

The Sports Data Service already has API and loader support for `giro-d-italia`, so the same stage-card flow used for Tour and La Vuelta can consume this bundle once it is populated.

The current builder reads the official Giro route calendar page and normalizes it into the same bundle shape used by the other grand tours.
