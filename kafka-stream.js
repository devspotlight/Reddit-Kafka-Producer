/**
 * 4. Fetches comments from a subreddit and sends it to kafka.
 * To be ran as a worker.
 */

/* global process, require, setTimeout, setInterval, clearInterval */

const fs = require('fs')

const axios = require('axios')
const Kafka = require('no-kafka')
const queue = require('async.queue')

const ProfileScraper = require('./profile-scraper')
const formatComment = require('./format-comment')

require('dotenv').config()

// TODO: Use if (NODE_ENV === 'production') {...

/**
 * Async fn to fetch 100 comments from `subreddit`
 * @param subreddit to fetch
 * @param cb callback to process each subreddit comment data
 * @param after has no effect (optional)
 * @todo remove ^
 * @returns {Promise<*>} empty object `{}` or { error }
 */
async function fetch100Subreddit (subreddit, cb, after) {
  // Fetches reddit.com/${subreddit}/comments.json?limit=100
  try {
    console.debug('fetch100Subreddit: fetching', subreddit)
    let path = `https://www.reddit.com/r/${subreddit}/comments.json?limit=100`

    if (typeof after !== 'undefined') {
      path += `?after=${after}`
    }

    const response = await axios.get(path)
    const { data } = response.data

    // Processes each `data.children.data` (comment data) with given callback.
    data.children.forEach(comment => cb(comment.data))

    // Returns `{}`.
    return {}
    // TODO: Should skip this?
  } catch (error) {
    console.error('fetch100Subreddit: error!', error)
    return { error }
  }
}

async function main () {
  // try {
    // Loads env vars with info for connecting to Kafka.
    const url = process.env.KAFKA_URL
    const cert = process.env.KAFKA_CLIENT_CERT
    const key = process.env.KAFKA_CLIENT_KEY

    // Overwrites local files to use as Kafka credentials.
    fs.writeFileSync('./client.crt', cert)
    fs.writeFileSync('./client.key', key)

    // Creates Kafka producer.
    const producer = new Kafka.Producer({
      clientId: 'reddit-comment-producer',
      connectionString: url.replace(/\+ssl/g, ''),
      ssl: {
        certFile: './client.crt',
        keyFile: './client.key'
      }
    })

    // Creates a ProfileScraper.
    const scraper = new ProfileScraper()

    /* Queue to send comments to Kafka topic */ // See https://caolan.github.io/async/docs.html#queue
    const q = queue(
      /**
       * `q` worker
       * @param message Reddit comment JSON data expected
       * @param cb callback to process comment data
       * @returns {Promise<void>}
       */
      async (message, cb) => {
        // Sends stringified JSON `message` to Kafka (async fn) after 500 ms.
        setTimeout(async () => {
          try {
            await producer.send({
              topic: 'northcanadian-72923.reddit-comments',
              partition: 0,
              message: {
                value: JSON.stringify(message)
              }
            })
            console.debug('worker q: sent comment from', message.author)
            cb()
          } catch (e) {
            console.error('worker: error!', e)
            cb()
          }
        }, 500)
      }, 1)

    await producer.init()
    console.info('worker: Connected to Kafka at', process.env.KAFKA_URL)

    let interval

    /**
     * Fn (to be ran at interval) for
     */
    const stream = () => {
      // If the queue is over 500 elements long, the interval stops.
      if (q.length() > 500) {
        clearInterval(interval)
        // Re-starts the 1s interval when the last item from queue `q` has returned from its worker.
        q.drain = () => { // See https://caolan.github.io/async/docs.html#QueueObject
          interval = setInterval(stream, 1000)
        }
      } else {
        // Gets 100 comments from 'politics' subreddit.
        fetch100Subreddit(
          'politics',
          /**
           * Async callback to process 'politics' subreddit comment data.
           * Fetches comment author's profile data and formats the comment, before queueing it to be sent to Kafka.
           * @param comment
           * @returns {Promise<void>}
           */
          async (comment) => {
            const profile = await scraper.fetchProfile(comment.author)
            const fullComment = formatComment(profile, comment)

            if (profile.error) {
              console.error("fetch100Subreddit('politics') callback: error!", profile.error)
            } else {
              // Pushes `comment` task to `q` queue (to be sent as message to Kafka).
              q.push(fullComment)
            }
          })
      }
    }

    // Starts the 1s interval.
    interval = setInterval(stream, 1000)
  // } catch (e) {
  //   console.error('worker error!', e)
  // }
}

main()
