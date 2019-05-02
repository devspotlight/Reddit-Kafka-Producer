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

let wait = ms => new Promise(resolve => setTimeout(resolve, ms));

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

    /**
     * Fn to read (all) rows from `profiles2` table (10 at a time)
     * and extract each comment to send to Kafka (with the right structure)
     */
    const loop10profiles = () => {
      // Reads 10 rows from `profiles2` table.
      cursor.read(
        10,
        /**
         * Anon callback to process all comments in these 10 rows from `profiles2`
         * @param err
         * @param rows
         */
        (err, rows) => {
          if (err) {
            console.error('kafka-export.js loop10profiles: error!', err)
            throw err
          }
          console.info('kafka-export.js loop10profiles: next ≤10 rows of profiles:\n', rows.map(p => {
            return { name: p.data.name, comments: p.data.comments.length }
          }))

          // Finishes loop when all rows are processed.
          if (rows.length === 0) {
            console.info('kafka-export.js loop10profiles: all rows processed!')
            kafkaQ.drain = () => {}
            client.release()
            return
          }

          // Constructs `comments` from each profile `row` and inserts each it into the `comments2` table.
          rows.forEach((row) => {
            const profile = row.data
            const comments = profile.comments

            let recentComments = [] // Start a data queue

            // console.debug('kafka-export.js loop10profiles forEach row: transforming', profile.name, 'into full comment')
            if (comments && comments.length > 0) {
              /* Nested forEach */
              comments.forEach((comment) => {
                let fullComment = formatComment(profile, comment)

                // Sets is_bot and is_troll (coming originally from bots.csv).
                fullComment.is_bot = comment.isBot
                fullComment.is_troll = comment.isTroll

                // Attaches last ≤20 user comment Reddit ids to this comment.
                fullComment.recent_comments = recentComments

                // Marks record as training data.
                fullComment.is_training = true

                // console.debug(
                //   'kafka-export.js loop10profiles forEach row > forEach comment: fullComment [ link_id, recent_comments ]',
                //   [fullComment.link_id, fullComment.recent_comments]
                // )
                kafkaQ.push(fullComment)

                // Keeps recentComments data queue at length 20.
                recentComments.push(comment.link_id)
                if (recentComments.length > 20) recentComments.shift()
              })
            }
          })

          // Start loop again if the `kafkaQ` is getting empty.
          console.debug('kafka-export.js loop10profiles: kafkaQ.length()', kafkaQ.length())
          if (kafkaQ.length() === 0) {
            console.debug('kafka-export.js loop10profiles: kafkaQ is empty! Restarting loop10profiles()')
            loop10profiles()
          }
        })
    }
    console.debug('loop10profiles defined')

    // To restart loop when/if `kafkaQ` gets empty
    if (NODE_ENV === 'production') {
      kafkaQ.drain = loop10profiles
      console.debug('kafkaQ.drain = loop10profiles')
    } else {
      kafkaQ.drain = () => { console.debug('kafka-export.js loop10profiles: kafkaQ drained.') }
    }

    // Starts first loop.
    loop10profiles()
    console.debug('loop10profiles started!')
  } catch (e) {
    console.error('kafka-export.js: error!', e)
  }
}

main()
