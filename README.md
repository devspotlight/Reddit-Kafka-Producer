# Reddit-Kafka-Producer

This repository is a collection of utilities and scripts dedicated to scraping comment and profile information from the reddit API and streaming it to kafka.

- `profile-scraper.js`: class for accessing data from the Reddit API.
- `profile.js`: (1) reads from a CSV file, fetches profile data, and dumps it to a database.
- `format-comment.js`: function for formatting JSON response data.
- `kafka-export.js`: (2) inserts JSON data to a table with a schema.
- `kafka-stream.js`: (3) fetches comments from a subreddit and sends them to Kafka. (See Procfile)

## Configuration

The following environment variables are required for the numbered scripts to run
properly (may use .env file):

- `PGUSER`
- `PGPASSWORD`
- `PGHOST`
- `PGPORT` (defaults to 5432)
- `PGDATABASE`

> See https://node-postgres.com/features/connecting

and:

- `KAFKA_CLIENT_CERT`
- `KAFKA_CLIENT_CERT_KEY`
- `KAFKA_URL`

> Only the latter KAFKA_ ones are needed for the export and stream scripts.

## Local Development

> Still depends on an actual Kafka cluster but won't send anything to it in dev mode.

Create a .env with values for the env vars listed in the previous section.

Install all the dependencies:

```console
$ npm i --dev
```
> If `--dev` isn't used, your local installation will lack linting and CSV dump abilities.

- You may run `profile.js` if needed like:
    
    ```console
    $ node profile.js
    ```

- For a CSV dump of `kafka-export.js`:

    ```console
    $ npm run dev-dump
    ```

    > `csv-writter` encodes `"` inside strings as `""` which seems to be a CSV-specific practice. You may want to run
    > ```console
    >  $ sed -i -e 's/""/\\"/g' training-dump.csv
    >  ```
    > to replace them for `\"`

- Try the main (worker) app with:

    ```console
    $ npm run worker
    # Same as `node kafka-stream.js` (see package.json scripts)
    ```

- Lint chec before committing changes:

    ```console
    $ npm run lint
    ```

## Deploy on Heroku

Most of the env vars mentioned before are set automatically by Heroku Pg and Kafka addons, you must separately set:

```console
$ heroku config:set NODE_ENV=production
```

The kafka-stream.js worker expects [log compaction](https://devcenter.heroku.com/articles/kafka-on-heroku#log-compaction) on the Kafka topic.

> Also unpack `DATABASE_URL` e.g. with https://metacpan.org/pod/Env::Heroku ?
  See https://devcenter.heroku.com/articles/heroku-postgresql#provisioning-heroku-postgres

### Running scripts on Heroku
This can be achieved with one-off Dynos When needed:

```console
$ heroku run bash
...
~ $ node profile.js # or kafka-export.js
  ... # Do your thing.
~ $ exit # when done
```

> See https://devcenter.heroku.com/articles/one-off-dynos
