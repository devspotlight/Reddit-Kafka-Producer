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

      CREATE TABLE IF NOT EXISTS profiles (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        data JSON
      );
    `

    await pool.query(createTableText)

    const dbQ = queue(async (profile , cb) => {
      console.log('inserting', profile.name)
      await pool.query('INSERT INTO profiles(data) VALUES($1)', [profile])
      cb()
    })

    const redditQ = queue(async ({ fn, username }, cb) => {
      const profile = await fn(username)

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
      const username = line.split(',')[0]
      redditQ.push({ fn: scraper.scrapeProfile.bind(scraper), username })
    })
  } catch (e) {
    console.log(e)
  }
}

main()
