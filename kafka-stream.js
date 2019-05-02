/**
 * 3. Fetches comments from a subreddit and sends it to kafka.
 * To be ran as a worker.
 */

/* global process, require, setTimeout, setInterval, clearInterval */

const fs = require('fs')

const axios = require('axios')
const queue = require('async.queue')
const Kafka = require('no-kafka')

const ProfileScraper = require('./profile-scraper')
const formatComment = require('./format-comment')

require('dotenv').config()

let wait = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Async fn to fetch 100 comments from `subreddit`
 * @param subreddit to fetch
 * @param each callback to process each subreddit comment data
 * @param after has no effect (optional)
 * @todo remove ^
 * @returns {Promise<*>} empty object `{}` or { error }
 */
async function fetch100Subreddit (subreddit, each, after) {
  // Fetches reddit.com/${subreddit}/comments.json?limit=100
  try {
    console.debug('fetch100Subreddit: fetching', subreddit)
    let path = `https://www.reddit.com/r/${subreddit}/comments.json?limit=100`

    if (typeof after !== 'undefined') {
      path += `?after=${after}`
    }

    console.debug('fetch100Subreddit: getting', path)
    const response = await axios.get(path)
    const { data } = response.data
    console.debug('fetch100Subreddit: got', data.dist, 'comments')

    // Processes each `data.children.data` (comment data) with given callback.
    data.children.forEach(comment => each(comment.data))

    // Returns `{}`.
    return {}
    // TODO: Should skip this?
  } catch (error) {
    console.error('fetch100Subreddit: error!', error)
    return { error }
  }
}

/**
 * Async main fn allows us to await other async calls
 * i.e. `producer.init()` and `pool.connect()`
 * @returns {Promise<void>}
 */
async function main () {
  try {
    // Loads env vars (with info for connecting to Kafka).
    const NODE_ENV = process.env.NODE_ENV
    const url = process.env.KAFKA_URL
    const cert = process.env.KAFKA_CLIENT_CERT
    const key = process.env.KAFKA_CLIENT_KEY
    console.debug('consts')

    // Creates Kafka producer. (Overwrites local files to use as Kafka credentials.)
    fs.writeFileSync('./client.crt', cert)
    fs.writeFileSync('./client.key', key)
    const producer = new Kafka.Producer({
      clientId: 'reddit-comment-producer',
      connectionString: url.replace(/\+ssl/g, ''),
      ssl: {
        certFile: './client.crt',
        keyFile: './client.key'
      }
    })
    console.debug('Created Kafka producer')

    await producer.init()
    console.info('worker: Connected to Kafka at', process.env.KAFKA_URL)

    /* Queue to send comments to the Kafka topic */ // See https://caolan.github.io/async/docs.html#queue
    const kafkaQ = queue(
      /**
       * Async `kafkaQ` worker fn to send each comment to Kafka (via `producer`).
       * @param comment Reddit comment JSON data expected
       * @param cb callback invoked when done
       * @returns {Promise<void>}
       */
      async (comment, cb) => {
        // console.debug('kafka-stream.js kafkaQ: sending JSON.stringify of', comment.link_id, 'to Kafka')
        wait(500)
        // Try sending stringified JSON `comment` to Kafka (async fn) after 500 ms.
        if (NODE_ENV === 'production') {
          try {
            await producer.send({
              topic: 'northcanadian-72923.reddit-comments',
              partition: 0,
              message: { value: JSON.stringify(comment) }
            })
            console.debug('worker kafkaQ: sent comment', comment.link_id, 'by', comment.author)
            cb()
          } catch (e) {
            console.error('kafka-stream.js kafkaQ: comment submission error!', e)
            cb()
          }
        } else {
          console.debug('kafka-export.js kafkaQ: Would send JSON data for', comment.link_id, 'comment by', comment.author, 'to Kafka')
        }
        cb()
      }, 1)
    console.debug('kafkaQ defined')

    let interval

    // Creates a ProfileScraper.
    const scraper = new ProfileScraper()

    /**
     * Fn (to be ran at interval) for streaming comments as messages to the Kafka topic
     */
    const stream = () => {
      // If the queue is over 500 elements long, the interval stops.
      // TODO: What if the API is unavailable? `fetch100Subreddit` will keep running again and again but `kafkaQ` never grows?
      if (kafkaQ.length() > 500) {
        clearInterval(interval)
        console.debug('stream: suspending interval...')
        // Re-starts the interval when the last item from queue `kafkaQ` has returned from its worker.
        kafkaQ.drain = () => { // See https://caolan.github.io/async/docs.html#QueueObject
          console.debug('stream(): restarting interval.')
          interval = setInterval(stream, 1000)
        }
      } else {
        // Gets 100 comments from 'politics' subreddit.
        fetch100Subreddit(
          'politics',
          /**
           * Async callback to process each comment in the 'politics' subreddit.
           * Fetches comment author's profile data and formats the comment, before queueing it to be sent to Kafka.
           * @param comment
           * @returns {Promise<void>}
           */
          async (comment) => {
            // console.debug("stream fetch100Subreddit('politics') callback: processing", comment.link_id)
            const profile = await scraper.fetchProfile(comment.author)

            if (profile.error) {
              console.error("stream fetch100Subreddit('politics') callback: error!", profile.error)
            } else {
              const fullComment = formatComment(profile, comment)

              // Pushes `comment` task to `kafkaQ` queue (to be sent as message to Kafka).
              kafkaQ.push(fullComment)
            }
          })
      }
    }
    console.debug('stream defined')

    // Starts the 1s interval.
    interval = setInterval(stream, 1000)
    console.debug('interval started!')
  } catch (e) {
    console.error('worker error!', e)
  }
}

main()
