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

let wait = ms => new Promise(resolve => setTimeout(resolve, ms))

/**
 * Async fn to fetch and process 100 comments from `subreddit`
 * @param subreddit to fetch
 * @param each callback to process the subreddit comment data
 * @returns {Promise<*>} empty object `{}` or { error }
 */
async function fetch100Subreddit (subreddit, each) {
  // Fetches reddit.com/${subreddit}/comments.json?limit=100
  try {
    console.debug('worker: fetching 100 comments from subreddit', subreddit)
    let path = `https://www.reddit.com/r/${subreddit}/comments.json?limit=100`

    console.debug('worker: requesting', path)
    const response = await axios.get(path)
    const { data } = response.data
    console.debug('worker: received', data.dist, 'comments')

    // Processes each `data.children.data` (comment data) with given callback.
    data.children.forEach(comment => each(comment.data))

    // Returns `{}`.
    return {}
    // TODO: Should skip this?
  } catch (error) {
    console.error('worker: fetch error!', error)
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
    console.info('worker: connected to Kafka at', process.env.KAFKA_URL)

    /* Queue to send comments to the Kafka topic */ // See https://caolan.github.io/async/docs.html#queue
    const kafkaQ = queue(
      /**
       * Async `kafkaQ` worker fn to send each comment to Kafka (via `producer`).
       * @todo https://github.com/oleksiyk/kafka/blob/master/README.ts.md#batching-grouping-produce-requests
       * @param comment Reddit comment JSON data expected
       * @param cb callback invoked when done
       * @returns {Promise<void>}
       */
      async (comment, cb) => {
        // console.debug('worker: producing JSON.stringify of', comment.link_id, 'for Kafka')
        wait(500)
        // Try sending stringified JSON `comment` to Kafka (async fn) after 500 ms.
        if (NODE_ENV === 'production') {
          try {
            let result = await producer.send({
              topic: 'northcanadian-72923.reddit-comments',
              partition: 0,
              message: { value: JSON.stringify(comment) }
            })
            console.debug('worker: Kafka producer sent comment', comment.link_id, '- offset [0]', result[0].offset)
            cb()
          } catch (e) {
            console.error('worker: Kafka producer submission error!', e)
            cb()
          }
        } else {
          console.debug('worker: would produce/send JSON data for', comment.link_id, 'comment by', comment.author, 'to Kafka. Q len', kafkaQ.length())
          cb()
        }
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
        console.debug('worker interval: suspending...')
        // Re-starts the interval when the last item from queue `kafkaQ` has returned from its worker.
        kafkaQ.drain = () => { // See https://caolan.github.io/async/docs.html#QueueObject
          console.debug('worker interval: restarting.')
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
            // console.debug("worker stream: fetch100Subreddit('politics') callback is processing", comment.link_id)
            const profile = await scraper.fetchProfile(comment.author)

            if (profile.error) {
              console.error("worker stream: fetch100Subreddit('politics') callback error!", profile.error)
            } else {
              const fullComment = formatComment(profile, comment)

              // Pushes `comment` task to `kafkaQ` queue (to be sent as message to Kafka).
              kafkaQ.push(fullComment)
            }
          }
        )
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
