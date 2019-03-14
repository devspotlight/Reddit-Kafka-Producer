# Reddit-Kafka-Producer

This repository is a collection of scripts dedicated to scraping comment and
profile information from the reddit API and streaming it to kafka.

- `profile.js`: reads from a CSV file, fetches profile data, and saves to a
  database.
- `subreddit.js`: fetches comments and user data from a subreddit and saves to a
  database.
- `db-export.js`: inserts JSON data to a table with a schema.
- `kafka-stream.js`: fetches comments from a subreddit and sends it to kafka.
- `profile-scraper.js`: a class for accessing data from the reddit API.
- `format-comment.js`: a function for formatting JSON response data.

The following environment variables are required for the scripts to run
properly:

- `PGHOST`
- `PGUSER`
- `PGDATABASE`
- `PGPASSWORD`
- `PGPORT`
- `KAFKA_CLIENT_CERT`
- `KAFKA_CLIENT_CERT_KEY`
- `KAFKA_URL`
