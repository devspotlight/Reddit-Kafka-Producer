/* global process, require, setTimeout */

const { Pool } = require('pg')
const Cursor = require('pg-cursor')
const queue = require('async.queue')

const formatComment = require('./format-comment')

require('dotenv').config()

async function main () {
  try {
    const pool = new Pool()
    const client = await pool.connect()

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

    const dbQ = queue(async ({ comment }, cb) => {
      console.log('inserting', comment[20])
      try {
        await pool.query(insertQuery, comment)
      } catch (e) {
        console.log(e)
      }
      cb()
    })

    const handleRow = (row) => {
      const profile = row.data
      const comments = profile.comments

      console.log('handling ', profile.name)

      if (comments && comments.length > 0) {
        comments.forEach((c) => {
          const comment = formatComment(profile, c)
          dbQ.push({ comment: Object.values(comment) })
        })
      }
    }

    const cursor = client.query(new Cursor('select * from profiles2'))

    const loop = () => {
      cursor.read(10, (err, rows) => {
        console.log('next cursor')
        if (err) throw err

        if (rows.length === 0) {
          dbQ.drain = () => {}
          console.log('done')
          client.release()
          return
        }

        rows.forEach(handleRow)

        if (dbQ.length() === 0) {
          loop()
        }
      })
    }

    dbQ.drain = loop
    loop()
  } catch (e) {
    console.log(e)
  }
}

main()
