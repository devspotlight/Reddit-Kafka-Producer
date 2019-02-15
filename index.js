/* global process, require, setTimeout */

const fs = require('fs')
const readline = require('readline')

const { Pool } = require('pg')
const queue = require('async.queue')

const ProfileScraper = require('./profile-scraper')

require('dotenv').config()

async function main () {
  try {
    const pool = new Pool()

    const createTableText = `
      CREATE EXTENSION IF NOT EXISTS "pgcrypto";

      CREATE TABLE IF NOT EXISTS profiles2 (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        data JSON
      );
    `

    await pool.query(createTableText)

    const dbQ = queue(async (profile , cb) => {
      console.log('inserting', profile.name)
      try {
        await pool.query('INSERT INTO profiles2(data) VALUES($1)', [profile])
      } catch (e) {
        console.log(profile.name, e)
      }
      cb()
    })

    const redditQ = queue(async ({ fn, username, isBot, isTroll }, cb) => {
      const profile = await fn(username, isBot, isTroll)

      if (!profile.error) {
        dbQ.push(profile)
      }

      cb()
    })

    const scraper = new ProfileScraper()

    const lines = readline.createInterface({
      input: fs.createReadStream('bots.csv')
    })

    lines.on('line', line => {
      const l = line.split(',')
      const username = l[0]
      const isBot = l[1] === 'TRUE'
      const isTroll = l[2] === 'TRUE'

      redditQ.push({
        fn: scraper.scrapeProfile.bind(scraper),
        username,
        isBot,
        isTroll
      })
    })
  } catch (e) {
    console.log(e)
  }
}

main()
