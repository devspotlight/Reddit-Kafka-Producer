/**
 * 2. Inserts JSON data to the database (`comments2` table).
 * To run as a script (once, when needed).
 */

/* global process, require */

const fs = require('fs')

const { Pool } = require('pg')
const Cursor = require('pg-cursor')
const queue = require('async.queue')
const Kafka = require('no-kafka')

const formatComment = require('./format-comment')

require('dotenv').config()

let wait = ms => new Promise(resolve => setTimeout(resolve, ms))

/**
 * Async fn to fetch and process comments from `n` Reddit profiles
 * (previously saved in a db table `profiles2`)
 * @param client pg connection @todo needed? Maybe its' in cursor.client
 * @param cursor for pg db connection
 * @param n how many
 * @param each callback to process the subreddit comment data
 * @returns {Promise<*>} empty object `{}` or { error }
 */
async function fetchProfiles (client, cursor, n, each) {
  cursor.read(
    n,
    /**
     * Anon callback to process all comments in these n rows from `profiles2`
     * @param err
     * @param rows
     */
    (err, rows) => {
      if (err) {
        console.error('kafka-export.js fetchProfiles: error!', err)
        throw err
      }
      // console.debug('kafka-export.js fetchProfiles: next', n, 'rows of profiles:\n', rows.map(p => {
      //   return { name: p.data.name, comments: p.data.comments.length }
      // }))

      // Processes each profile `row`.
      rows.forEach(each)
    })
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

    // Connects to Potsgres using default env vars. See https://node-postgres.com/features/connecting
    const pool = new Pool({ ssl: true })
    const client = await pool.connect()
    console.info('kafka-export.js: Connected to Postgres at', process.env.PGHOST)

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
    console.info('kafka-export.js: Connected to Kafka at', process.env.KAFKA_URL)

    /* Db cursor to read from `profiles2` table */ // See https://node-postgres.com/api/cursor
    console.info('kafka-export.js: Querying', 'SELECT * FROM profiles2')
    const cursor = client.query(new Cursor('SELECT * FROM profiles2'))

    /* Queue to insert comments into the db */
    const kafkaQ = queue(
      /**
       * Async `kafkaQ` worker fn to send each comment to Kafka (via `producer`).
       * @todo https://github.com/oleksiyk/kafka/blob/master/README.ts.md#batching-grouping-produce-requests
       * @param comment Reddit comment JSON data expected
       * @param cb callback invoked when done
       * @returns {Promise<void>}
       */
      async (comment, cb) => {
        // console.debug('kafka-export.js kafkaQ: sending Object.values of', comment.link_id, 'to Kafka')
        wait(500)
        // Try sending stringified JSON `comment` to Kafka (async fn) after 500 ms.
        if (NODE_ENV === 'production') {
          try {
            let result = await producer.send({
              topic: 'northcanadian-72923.reddit-comments',
              partition: 0,
              message: { value: JSON.stringify(comment) }
            })
            console.debug('worker kafkaQ: sent comment', comment.link_id, '- [0] offset', result[0].offset)
            cb()
          } catch (e) {
            console.error('kafka-export.js kafkaQ: comment submission error!', e)
            cb()
          }
        } else {
          console.debug('kafka-export.js kafkaQ: Would send JSON data for', comment.link_id, 'comment by', comment.author, 'to Kafka. Q len', kafkaQ.length())
          cb()
        }
      }, 1)
    console.debug('kafkaQ defined')

    let interval

    // // Creates a ProfileScraper.
    // const scraper = new ProfileScraper()

    /**
     * Fn (to be ran at interval) for streaming comments as messages to the Kafka topic
     */
    const stream = () => {
      // If the queue is over 99 elements long, the interval stops.
      // TODO: What if the API is unavailable? `fetchProfiles` will keep running again and again but `kafkaQ` never grows?
      if (kafkaQ.length() > 99) {
        clearInterval(interval)
        console.debug('kafka-export.js stream: suspending interval...')
        // Re-starts the interval when the last item from queue `kafkaQ` has returned from its worker.
        kafkaQ.drain = () => { // See https://caolan.github.io/async/docs.html#QueueObject
          console.debug('kafka-export.js stream: restarting interval.')
          interval = setInterval(stream, 1000)
        }
      } else {
        // Fetches profiles ONE BY ONE from the db (and all of their comments).
        fetchProfiles(
          client,
          cursor,
          1,
          /**
           * Async callback to process all comments in each project fetched.
           * Fetches comment author's profile data and formats the comment, before queueing it to be sent to Kafka.
           * @param profile
           * @returns {Promise<void>}
           */
          (profile) => {
            console.debug('kafka-export.js stream fetchProfiles(10) callback: processing profile', profile.data.name, 'with', profile.data.comments.length, 'comments')
            const comments = profile.data.comments

            comments.forEach((comment) => {
              const fullComment = formatComment(profile, comment)
              // console.debug('kafka-export.js stream fetchProfiles(10) callback: sending', fullComment)

              // Pushes `comment` task to `kafkaQ` queue (to be sent as message to Kafka).
              kafkaQ.push(fullComment)
            })
          }
        )
      }
    }
    console.debug('stream defined')

    // Starts the 1s interval.
    interval = setInterval(stream, 1000)
    console.debug('interval started!')
  } catch (e) {
    console.error('kafka-export.js: error!', e)
  }
}

main()
