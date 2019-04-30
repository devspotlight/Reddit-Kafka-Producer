/**
 * 2. Fetches comments and user data from a subreddit and saves to the database (`profiles` table).
 * To run as a script (once, when needed).
 */

/* global process, require, setTimeout */

const axios = require('axios')
const { Pool } = require('pg')
const queue = require('async.queue')

const ProfileScraper = require('./profile-scraper')

require('dotenv').config()

/**
 * Async fn to fetch all the comments in `subreddit`
 * @param subreddit to fetch
 * @param cb callback to process each subreddit comment data
 * @param after last comment id (https://www.reddit.com/dev/api#fullnames) fetched (optional)
 * @returns {Promise<*>} empty object `{}` or { error }
 */
async function fetchSubreddit (subreddit, cb, after) {
  // Waits 1 sec. so reddit.com doesn't block us.
  await new Promise(resolve => setTimeout(resolve, 1000))

  // Fetches reddit.com/${subreddit}/comments.json
  try {
    console.info('subreddit.js fetchSubreddit: fetching', subreddit)
    let path = `https://www.reddit.com/r/${subreddit}/comments.json`

    if (typeof after !== 'undefined') {
      path += `?after=${after}`
    }

    const response = await axios.get(path)
    const { data } = response.data // data = response.data.data

    // Processes each `data.children.data` (comment data) with given callback.
    data.children.forEach(comment => cb(comment.data))

    // Calls itself recursively for any comments left to fetch.
    if (data.after !== null) {
      return fetchSubreddit(subreddit, cb, after)
      // TODO: BUG? Should use `data.after` ^
    }

    // Returns `{}` when there's no more comments left to fetch.
    return {}
    // TODO: Should skip this?
  } catch (error) {
    console.error('subreddit.js fetchSubreddit: error!', error)
    return { error }
  }
}

async function main () {
  try {
    // Connects to Potsgres using default env vars. See https://node-postgres.com/features/connecting
    const pool = new Pool()
    // If needed, adds pgcrypto extension to the db (https://www.postgresql.org/docs/current/pgcrypto.html)
    // and creates `profiles` table.
    const createTableText = `
      CREATE EXTENSION IF NOT EXISTS "pgcrypto";

      CREATE TABLE IF NOT EXISTS profiles (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        data JSON
      );
    `
    await pool.query(createTableText)

    /* Queue to insert fetched profile to db */ // See https://caolan.github.io/async/docs.html#queue
    const dbQ = queue(async (profile, cb) => {
      console.info('subreddit.js dbQ: inserting', profile.name)
      await pool.query('INSERT INTO profiles(data) VALUES($1)', [profile])
      cb()
    })

    // Creates a ProfileScraper.
    const scraper = new ProfileScraper()

    // Fetches 'politics' subreddit
    fetchSubreddit(
      'politics',
      /**
       * Async callback to process 'politics' subreddit comment data.
       * Fetches comment author's profile data before queueing it to be inserted to db.
       * @param comment
       * @returns {Promise<void>}
       */
      async (comment) => {
        const profile = await scraper.fetchProfile(comment.author)

        profile.comments = [ comment ]
        profile.isBot = false
        if (profile.error) {
          console.error("fetchProfile('politics') callback: error!", profile.error)
        } else {
          // Pushes `profile` task to `dbQ` queue (to be inserted to the db `profiles` table).
          dbQ.push(profile)
        }
      })
  } catch (e) {
    console.error('subreddit.js: main() error!', e)
  }
}

main()
