/* global process, require, setTimeout */

const fs = require('fs')

const axios = require('axios')
const Kafka = require('no-kafka')
const queue = require('async.queue')

const ProfileScraper = require('./profile-scraper')
const formatComment = require('./format-comment')

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

    // if (data.after !== null) {
    //   return fetchSubreddit(subreddit, cb, after)
    // }

    return {}
  } catch (error) {
    return { error }
  }
}

async function main () {
  try {
    const cert = process.env.KAFKA_CLIENT_CERT
    const key  = process.env.KAFKA_CLIENT_CERT_KEY
    const url  = process.env.KAFKA_URL

    fs.writeFileSync('./client.crt', process.env.KAFKA_CLIENT_CERT)
    fs.writeFileSync('./client.key', process.env.KAFKA_CLIENT_CERT_KEY)

    const producer = new Kafka.Producer({
      clientId: 'reddit-comment-producer',
      connectionString: url.replace(/\+ssl/g,''),
      ssl: {
        certFile: './client.crt',
        keyFile: './client.key'
      }
    })

    const scraper = new ProfileScraper()

    const q = queue(async (message, cb) => {
      try {
        await producer.send({
          topic: 'reddit-comments',
          partition: 0,
          message
        })
        cb()
      } catch (e) {
        console.log(e)
        cb()
      }
    })

    await producer.init()
    console.log('connected!')

    fetchSubreddit('politics', async (c) => {
      const author = c.author
      const profile = await scraper.fetchProfile(`u/${author}`)

      console.log(author)

      const comment = formatComment(profile, c)

      if (!profile.error) {
        q.push(comment)
      }
    })
  } catch (e) {
    console.log(e)
  }
}

main()
