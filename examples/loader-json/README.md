# loader-json

Demonstrates loading JSON values from a client-side JSON file with `FileUrisJSONLoader`.

This example selects each object under `/items/*`, captures `/metadata` for the batch modules, and writes each selected JSON object to `build/loader-json.jsonl`.

Run with `./gradlew corb -PcorbOptionsFile=loader-json/job.properties`.
