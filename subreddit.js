/* global process, require, setTimeout */

const axios = require('axios')
const { Pool } = require('pg')
const queue = require('async.queue')

const ProfileScraper = require('./profile-scraper')

require('dotenv').config()

async function fetchSubreddit (subreddit, cb, after) {
  await new Promise(resolve => setTimeout(resolve, 1000))

  try {
    console.log(subreddit)
    let path = `https://www.reddit.com/r/${subreddit}/comments.json`

    if (typeof after !== 'undefined') {
      path = `${path}?after=${after}`
    }

    const response = await axios.get(path)
    const { data } = response.data
    data.children.forEach(comment => cb(comment.data))

    if (data.after !== null) {
      return fetchSubreddit(subreddit, cb, after)
    }

    return {}
  } catch (error) {
    return { error }
  }
}

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

    const scraper = new ProfileScraper()

    fetchSubreddit('politics', async (comment) => {
      const author = comment.author
      const profile = await scraper.fetchProfile(`u/${author}`)
      console.log(author)
      profile.comments = [ comment ]
      profile.isBot = false

      if (!profile.error) {
        dbQ.push(profile)
      }
    })

  } catch (e) {
    console.log(e)
  }
}

if (process.env.RUN_SCRIPT === 'true') {
  main()
}
