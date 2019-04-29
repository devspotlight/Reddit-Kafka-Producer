/**
 * 3. Inserts JSON data to a table with a schema.
 * To run as a script (once, when needed).
 */

/* global process, require */

const { Pool } = require('pg')
const Cursor = require('pg-cursor')
const queue = require('async.queue')

const formatComment = require('./format-comment')

require('dotenv').config()

async function main () {
  try {
    // Connects to Potsgres using default env vars. See https://node-postgres.com/features/connecting
    const pool = new Pool()
    const client = await pool.connect()
    // If needed, adds pgcrypto extension to the db (https://www.postgresql.org/docs/current/pgcrypto.html)
    // and creates `comments` table.
    const createTableText = `
      CREATE EXTENSION IF NOT EXISTS "pgcrypto";

      CREATE TABLE IF NOT EXISTS comments (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        author_link_karma integer,
        author_comment_karma integer,
        author_created_at integer,
        author_verified boolean,
        author_has_verified_email boolean,
        subreddit_id text,
        approved_at_utc integer,
        edited integer,
        mod_reason_by text,
        banned_by text,
        author_flair_type text,
        removal_reason text,
        link_id text,
        author_flair_template_id text,
        likes integer,
        banned_at_utc integer,
        mod_reason_title text,
        gilded integer,
        archived boolean,
        no_follow boolean,
        author text,
        num_comments integer,
        score integer,
        over_18 boolean,
        controversiality integer,
        body text,
        link_title text,
        downs integer,
        is_submitter boolean,
        subreddit text,
        num_reports integer,
        created_utc integer,
        quarantine boolean,
        subreddit_type text,
        ups integer,
        is_bot boolean,
        is_troll boolean
      );
    `
    await client.query(createTableText)

    // SQL query to insert data to the `comments` table created above.
    const insertQuery = `
      INSERT INTO comments(
        author_link_karma,
        author_comment_karma,
        author_created_at,
        author_verified,
        author_has_verified_email,
        subreddit_id,
        approved_at_utc,
        edited,
        mod_reason_by,
        banned_by,
        author_flair_type,
        removal_reason,
        link_id,
        author_flair_template_id,
        likes,
        banned_at_utc,
        mod_reason_title,
        gilded,
        archived,
        no_follow,
        author,
        num_comments,
        score,
        over_18,
        controversiality,
        body,
        link_title,
        downs,
        is_submitter,
        subreddit,
        num_reports,
        created_utc,
        quarantine,
        subreddit_type,
        ups,
        is_bot,
        is_troll
      ) VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        $15,
        $16,
        $17,
        $18,
        $19,
        $20,
        $21,
        $22,
        $23,
        $24,
        $25,
        $26,
        $27,
        $28,
        $29,
        $30,
        $31,
        $32,
        $33,
        $34,
        $35,
        $36,
        $37
      )
    `

    /* Queue to insert a comment to the db. */
    const dbQ = queue(
      /**
       * `dbQ` worker to insert `comment` data to the db according to `insertQuery`.
       * @param comment
       * @param cb
       * @returns {Promise<void>}
       */
      async ({ comment }, cb) => {
        console.info('db-export.js: inserting', comment[20])
        try {
          await pool.query(insertQuery, comment)
        } catch (e) {
          console.error('db-export.js dbQ: comment insertion error!', e)
        }
        cb()
      })

    /**
     * Fn to handle a row from the `profiles2` table
     * Uses `dbQ` to to the db.
     * @param row record with {data, comments}
     */
    const handleRow = (row) => {
      const profile = row.data
      const comments = profile.comments

      console.info('db-export.js: handling ', profile.name)
      if (comments && comments.length > 0) {
        comments.forEach((comment) => {
          const fullComment = formatComment(profile, comment)
          dbQ.push({ comment: Object.values(fullComment) })
        })
      }
    }

    /* Db cursor to read from `profiles2` table */ // See https://node-postgres.com/api/cursor
    const cursor = client.query(new Cursor('SELECT * FROM profiles2'))

    /**
     * Fn to read (all) rows from `profiles2` table
     */
    const loop = () => {
      // Reads 10 rows from `profiles2` table.
      cursor.read(10, (err, rows) => {
        if (err) {
          console.error('db-export.js: db loop error!', err)
          throw err
        }
        console.info('db-export.js: next <10 db rows...')

        if (rows.length === 0) {
          dbQ.drain = () => {}
          console.info('db-export.js: all rows processed!')
          client.release()
          return
        }

        rows.forEach(handleRow)

        // `loop()` again if the `dbQ` is getting empty.
        if (dbQ.length() === 0) {
          loop()
        }
      })
    }

    // Restarts `loop` when/if `dbQ` gets empty.
    dbQ.drain = loop

    // Starts first `loop` manually.
    loop()
  } catch (e) {
    console.error('db-export.js: error!', e)
  }
}

main()
