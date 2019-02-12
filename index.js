/* global require, setTimeout */

const fs = require('fs')
const readline = require('readline')

const axios = require('axios')
const { Pool } = require('pg')
const queue = require('async.queue')

function ProfileScraper () {}

ProfileScraper.prototype.fetchProfile = async function (username) {
  await new Promise(resolve => setTimeout(resolve, 1000))

  try {
    const path = `https://www.reddit.com/${username}/about.json`
    const response = await axios.get(path)
    return response.data.data
  } catch (error) {
    return { error }
  }
}

ProfileScraper.prototype.fetchComments = async function (username, after) {
  await new Promise(resolve => setTimeout(resolve, 1000))

  try {
    console.log(username, after)
    let path = `https://www.reddit.com/${username}/comments.json`

    if (typeof after !== 'undefined') {
      path = `${path}?after=${after}`
    }

    const response = await axios.get(path)
    const { data } = response.data

    const comments = data.children.map(child => {
      return child.data
    })

    if (data.after === null) {
      return comments
    }

    return [].concat(comments, await this.fetchComments(username, data.after))
  } catch (error) {
    console.log(error)
    return { error }
  }
}

ProfileScraper.prototype.scrapeProfile = async function (username) {
  const user = await this.fetchProfile(username)

  if (user.error) {
    return user
  }

  const comments = await this.fetchComments(username)

  return { ...user, comments, isBot: true }
}

async function main () {
  try {
    require('dotenv').config()

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
      input: fs.createReadStream('test.csv')
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
