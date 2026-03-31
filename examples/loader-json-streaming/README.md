# loader-json-streaming

Demonstrates loading JSON values from a client-side JSON file with `FileUrisStreamingJSONLoader`.

This example uses a root JSON array and sets `JSON-NODE=/*`, which selects each object in the array. It writes each selected JSON object to `build/loader-json-streaming.jsonl`.

Run with `./gradlew corb -PcorbOptionsFile=loader-json-streaming/job.properties`.
