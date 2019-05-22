# Reddit-Kafka-Producer

This repository is a collection of utilities and scripts dedicated to scraping comment and profile information from the reddit API and streaming it to kafka.

- `profile-scraper.js`: class for accessing data from the Reddit API.
- `profile.js`: (1) reads from a CSV file, fetches profile data, and saves to a database.
- `format-comment.js`: function for formatting JSON response data.
- `kafka-export.js`: (2) inserts JSON data to a table with a schema.
- `kafka-stream.js`: (3) fetches comments from a subreddit and sends it to kafka. (Used in Procfile)

## Configuration

The following environment variables are required for the 4 numbered scripts to run
properly (may use .env file):

- `PGUSER`
- `PGPASSWORD`
- `PGHOST`
- `PGPORT` (defaults to 5432)
- `PGDATABASE`

> See https://node-postgres.com/features/connecting

and:

- `KAFKA_CLIENT_CERT`
- `KAFKA_CLIENT_KEY`
- `KAFKA_URL`

## Local Development

Create a .env with values for the env vars listed in the previous section.

Install all the dependencies:

```console
$ npm i --dev
```
> If `--dev` isn't used, your local installation will lack linting and CSV dump abilities.

Run scripts 1-3 when needed like:

```console
$ node profile.js
```

or for a CSV dump of kafka-export.js:

```console
$ npm run dev-dump
```
> `csv-writter` encodes `"` inside strings as `""` which seems to be a CSV-specific practice. You may want to run
> ```console
>  $ sed -i -e 's/""/\\"/g' training-dump.csv
>  ```
> to replace them for `\"`

Run main (worker) app:

```console
$ npm run worker
# Same as `node kafka-stream.js` (See Procfile)
```

> Lint before committing changes:
> 
> ```console
> $ npm run lint
> ```

## Deploy on Heroku

Besides the env vars mentioned before, you must set:

```console
$ heroku config:set NODE_ENV=production
```

> Also unpack `DATABASE_URL` e.g. with https://metacpan.org/pod/Env::Heroku ?
  See https://devcenter.heroku.com/articles/heroku-postgresql#provisioning-heroku-postgres

### Running scripts on Heroku
This can be achieved with one-off dynos When needed:

```console
$ heroku run bash
...
~ $ node profile.js # or kafka-export.js
  ... # Do your thing.
~ $ exit # when done
```

> See https://devcenter.heroku.com/articles/one-off-dynos
