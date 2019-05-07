/**
 * 1. Reads from a CSV file, fetches profile data and comments, and saves to the database (`profiles2` table).
 * To run as a script (once, when needed).
 */

/* global process, require */

const fs = require('fs')
const readline = require('readline')

const { Pool } = require('pg')
const queue = require('async.queue')

const ProfileScraper = require('./profile-scraper')

require('dotenv').config()

async function main () {
  try {
    const NODE_ENV = process.env.NODE_ENV

    // Connects to Potsgres using default env vars. See https://node-postgres.com/features/connecting
    const pool = new Pool()
    // If needed, adds pgcrypto extension to the db (https://www.postgresql.org/docs/current/pgcrypto.html)
    // and creates `profiles2` table.
    const createTableText = `
      CREATE EXTENSION IF NOT EXISTS "pgcrypto";

      CREATE TABLE IF NOT EXISTS profiles2 (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        data JSON
      );
    `
    if (NODE_ENV === 'production') {
      await pool.query(createTableText)
    } else console.info('profile.js: Would', 'CREATE TABLE IF NOT EXISTS profiles2...')

    // Queues https://caolan.github.io/async/docs.html#queue

    /* Queue to insert scraped `profile` to db */
    const dbQ = queue(async (profile, cb) => {
      console.debug('profile.js dbQ worker: inserting profile for', profile.name)

      if (NODE_ENV === 'production') {
        try {
          await pool.query('INSERT INTO profiles2(data) VALUES($1)', [profile])
        } catch (e) {
          console.error('profile.js dbQ worker: insertion error!', e)
        }
      } else console.info('profile.js dbQ worker: Would', 'INSERT INTO profiles2(data) VALUES(...)', profile)
      cb()
    })

    // Creates a ProfileScraper.
    const scraper = new ProfileScraper()

    /* Queue to scrape a Reddit user profile
     * (awaits scraper.scrapeProfile which calls the Reddit API)
     * and queues the profile into `dbQ` */
    const redditQ = queue(async ({ username, isBot, isTroll }, cb) => {
      console.debug('profile.js redditQ worker: scraping profile for', username)

      const profile = await scraper.scrapeProfile(username, isBot, isTroll)

      if (profile.error) {
        console.error('profile.js redditQ worker: error!', profile.error)
      } else {
        dbQ.push(profile)
      }

      cb()
    })

    // Opens bots.csv file.
    const lines = readline.createInterface({
      input: fs.createReadStream('bots.csv')
    })

    // Reads and parses each CSV line. And...
    lines.on('line', line => {
      const l = line.split(',')
      const username = l[0]
      const isBot = l[1] === 'TRUE'
      const isTroll = l[2] === 'TRUE'

      // Queues scraping of each Reddit user profile.
      redditQ.push({
        username,
        isBot,
        isTroll
      })
    })
  } catch (e) {
    console.error('profile.js: main() error!', e)
  }
}

main()
