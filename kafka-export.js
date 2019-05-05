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
 * Fetches and queues comments from `n` Reddit profiles
 * (previously saved in a db table `profiles2`)
 * @param cursor for pg db connection
 * @param n how many
 * @param queue to process each comment
 */
function fetchProfiles (cursor, n, queue) {
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
      rows.forEach(
        /**
         * Async callback to process all comments in each profile fetched.
         * Fetches comment author's profile data and formats the comment, before queueing it to be sent to Kafka.
         * @param profile data row from db
         * @returns {Promise<void>}
         */
        (profile) => {
          console.debug('kafka-export.js: fetchProfiles(10) processing profile', profile.data.name, 'with', profile.data.comments.length, 'comments')
          const comments = profile.data.comments

          let recentComments = [] // Start a data queue

          comments.forEach((comment) => {
            const fullComment = formatComment(profile, comment)

            // Sets is_bot and is_troll (coming originally from bots.csv).
            fullComment.is_bot = profile.data.isBot
            fullComment.is_troll = profile.data.isTroll

            // Attaches last ≤20 user comment Reddit ids to this comment.
            fullComment.recent_comments = recentComments.slice()

            // Marks record as training data.
            fullComment.is_training = true

            // // console.debug('kafka-export.js: fetchProfiles(10) sending', fullComment)
            // console.debug(
            //   'kafka-export.js: forEach row > forEach comment fullComment [ link_id, recent_comments ]',
            //   [fullComment.link_id, fullComment.recent_comments]
            // )

            // Pushes `comment` task to `kafkaQ` queue (to be sent as message to Kafka).
            queue.push(fullComment)

            // Keeps recentComments data queue at length 20.
            recentComments.push(comment.link_id)
            if (recentComments.length > 20) recentComments.shift()
          })
        }
      )
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

    // Connects to Potsgres using default env vars. See https://node-postgres.com/features/connecting
    const pool = new Pool({ ssl: true })
    const client = await pool.connect()
    console.info('kafka-export.js: connected to Postgres at', process.env.PGHOST)

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
    console.info('kafka-export.js: connected to Kafka at', process.env.KAFKA_URL)

    /* Db cursor to read from `profiles2` table */ // See https://node-postgres.com/api/cursor
    console.info('kafka-export.js: querying', 'SELECT * FROM profiles2')
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
            console.debug('kafka-export.js: producer sent comment', comment.link_id, '- [0] offset', result[0].offset)
            cb()
          } catch (e) {
            console.error('kafka-export.js: producer submission error!', e)
            cb()
          }
        } else {
          console.debug('kafka-export.js: would produce/send JSON data for', comment.link_id, 'comment by', comment.author, '- Queue len', kafkaQ.length())
          cb()
        }
      }, 1)
    console.debug('kafkaQ defined')

    let interval

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
        // Fetches profiles from the db (and all of their comments). // TODO: Crashes with n > 1
        fetchProfiles(cursor, 1, kafkaQ) // NOTE: is async
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
