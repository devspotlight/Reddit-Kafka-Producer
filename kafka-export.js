/**
 * 2. Inserts JSON data to the database (`comments2` table).
 * To run as a script (once, when needed).
 */

/* global process, require, setTimeout, setInterval, clearInterval */

const fs = require('fs')

const { Pool } = require('pg')
const Cursor = require('pg-cursor')
const queue = require('async.queue')
const Kafka = require('no-kafka')

const { formatComment } = require('./format-comment')

require('dotenv').config()
const NODE_ENV = process.env.NODE_ENV

/**
 * From https://stackoverflow.com/a/39027151/761963
 * @param ms milliseconds to wait
 * @returns {Promise<any>}
 */
let wait = ms => new Promise(resolve => setTimeout(resolve, ms))

/**
 * Fetches and queues comments from `n` Reddit profiles
 * (previously saved in a db table `profiles2`)
 * @param cursor for pg db connection
 * @param n how many
 * @param queue to process each comment
 */
function fetchProfiles (cursor, n, queue) {
  // console.debug('kafka-export.js: fetchProfiles cursor.text n queue.concurrency', cursor.text, n, queue.concurrency)
  cursor.read(
    n,
    /**
     * Anon callback to process all comments in these n rows from `profiles2`
     * @param err See https://node-postgres.com/api/cursor
     * @param rows See https://node-postgres.com/api/cursor
     */
    (err, rows) => {
      // NOTE: There should be `n` `rows` at this point.
      if (err) {
        console.error('kafka-export.js: fetchProfiles error!', err)
        throw err
      }
      console.info('kafka-export: next', n, 'rows of profiles:\n', rows.map(p => {
        return { name: p.data.name, comments: p.data.comments.length }
      }))

      // Processes each profile `row`.
      rows.forEach(
        /**
         * Async callback to process all comments in each profile fetched.
         * Fetches comment author's profile data and formats the comment, before queueing it to be sent to Kafka.
         * @param profile data row from db
         * @returns {Promise<void>}
         */
        (profile) => {
          console.info('kafka-export: processing profile', profile.data.name, 'with', profile.data.comments.length, 'comments')
          const comments = profile.data.comments.reverse() // (`profile2` `comments` are expected in descending order.)

          let recentComments = [] // Start a data queue

          comments.forEach((comment) => {
            // console.debug('kafka-export.js: fetchProfiles x', n, 'comment', comment)

            const fullComment = formatComment(profile.data, comment)

            // Sets is_bot and is_troll (coming originally from bots.csv).
            fullComment.is_bot = profile.data.isBot
            fullComment.is_troll = profile.data.isTroll

            // Attaches (≤20) previous comments by the same author to this comment.
            fullComment.recent_comments = JSON.stringify(recentComments.slice(1, 21)) // JSON formatted string

            // Marks record as training data.
            fullComment.is_training = true

            // // console.debug('kafka-export.js: fetchProfiles x', n, 'sending comment', fullComment)
            // console.debug(
            //   'kafka-export.js: comment [ link_id, recent_comments ]',
            //   [fullComment.link_id, recentComments.slice(1, 21).map(c => { return { link_id: c.link_id, created_utc: c.created_utc } })]
            // )

            // Pushes comment as task to `queue` (to be sent as message to Kafka).
            queue.push(fullComment)

            // Adds full comment to recent comments queue (without augmenting fields to avoid recursive `recent_comments`).
            recentComments.push(formatComment(profile.data, comment))
            // Keeps `recentComments` data queue shifting and at max length 20.
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
    // Loads env vars for connecting to Kafka.
    const url = process.env.KAFKA_URL
    const cert = process.env.KAFKA_CLIENT_CERT
    const key = process.env.KAFKA_CLIENT_KEY

    // Connects to Potsgres using default env vars. See https://node-postgres.com/features/connecting
    const pool = new Pool({ ssl: true })
    const client = await pool.connect()
    console.info('kafka-export: connected to Postgres at', process.env.PGHOST)

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

    await producer.init()
    console.info('kafka-export: connected to Kafka at', process.env.KAFKA_URL)

    /* Db cursor to read from `profiles2` table */ // See https://node-postgres.com/api/cursor
    console.info('kafka-export: querying', 'SELECT * FROM profiles2')
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
        // console.debug('kafka-export.js: kafkaQ sending JSON.stringify of', comment.link_id, 'to Kafka')
        wait(500)
        // Try sending stringified JSON `comment` to Kafka (async fn) after 500 ms.
        if (NODE_ENV === 'production') {
          try {
            let result = await producer.send({
              topic: 'northcanadian-72923.reddit-comments',
              partition: 0,
              message: { value: JSON.stringify(comment) }
              // TODO: Don't JSON.stringify `message`? See https://www.npmjs.com/package/kafka-node#sendpayloads-cb
            })
            console.info('kafka-export: producer sent comment', comment.link_id, comment.created_utc, '- offset', result[0].offset)
            cb()
          } catch (error) {
            console.error('kafka-export.js: producer submission error!', error)
            cb()
          }
        } else {
          console.info('kafka-export: would produce/send JSON data for', comment.link_id, comment.created_utc, 'comment by', comment.author, '- Queue len', kafkaQ.length())
          cb()
        }
      }, 1)

    let interval

    /**
     * Fn (to be ran at interval) for streaming comments as messages to the Kafka topic
     */
    const stream = () => {
      // If the queue is over 99 elements long, the interval stops.
      // TODO: What if the API is unavailable? `fetchProfiles` will keep running again and again but `kafkaQ` never grows?
      if (kafkaQ.length() > 99) {
        clearInterval(interval)
        // console.debug('kafka-export.js: suspending interval...')
        // Re-starts the interval when the last item from queue `kafkaQ` has returned from its worker.
        kafkaQ.drain = () => { // See https://caolan.github.io/async/docs.html#QueueObject
          // console.debug('kafka-export.js: restarting interval.')
          interval = setInterval(stream, 1000)
        }
      } else {
        // Fetches profiles from the db and queues all of their comments for the Kafka producer.
        // TODO: Crashes with n > 1
        fetchProfiles(cursor, 1, kafkaQ)
        // TODO: The time interval isn't necessarily effective with `fetchProfiles` because it calls `cursor.read`...
      }
    }

    // Starts the 1s interval.
    interval = setInterval(stream, 1000)
  } catch (error) {
    console.error('kafka-export.js error!', error)
  }
}

main()
