/* global process, require, setTimeout */

const { Pool } = require('pg')
const queue = require('async.queue')

require('dotenv').config()

async function main () {
  try {
    const pool = new Pool()

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
        isBot boolean
      );
    `

    await pool.query(createTableText)

    const notBotQuery = `select data from profiles where (data -> 'isBot')::text = 'false';`
    const botQuery = `select data from profiles where json_array_length(data -> 'comments') < 700 and (data -> 'isBot')::text = 'true' limit 25;`

    const notBots = await pool.query(notBotQuery)
    const bots = await pool.query(botQuery)

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
        isBot
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
        $36
      )
    `

    const dbQ = queue(async ({ comment }, cb) => {
      console.log('inserting', comment[20])
      await pool.query(insertQuery, comment)
      cb()
    })

    function handleRows (rows) {
      rows.forEach(row => {
        const profile = row.data
        const comments = profile.comments

        comments.forEach(c => {
          const comment = [
            profile.link_karma,
            profile.comment_karma,
            profile.created_utc,
            profile.verified,
            profile.has_verified_email,
            c.subreddit_id,
            c.approved_at_utc,
            c.edited || 0,
            c.mod_reason_by,
            c.banned_by,
            c.author_flair_type,
            c.removal_reason,
            c.link_id,
            c.author_flair_template_id,
            c.likes,
            c.banned_at_utc,
            c.mod_reason_title,
            c.gilded,
            c.archived,
            c.no_follow,
            c.author,
            c.num_comments,
            c.score,
            c.over_18,
            c.controversiality,
            c.body,
            c.link_title,
            c.downs,
            c.is_submitter,
            c.subreddit,
            c.num_reports,
            c.created_utc,
            c.quarantine,
            c.subreddit_type,
            c.ups,
            profile.isBot
          ]

          dbQ.push({ comment })
        })
      })
    }

    handleRows(bots.rows)
    handleRows(notBots.rows)

  } catch (e) {
    console.log(e)
  }
}

if (process.env.RUN_SCRIPT === 'true') {
  main()
}
