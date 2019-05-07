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
const NODE_ENV = process.env.NODE_ENV

/**
 * From https://stackoverflow.com/a/39027151/761963
 * @param ms milliseconds to wait
 * @returns {Promise<any>}
 */
let wait = ms => new Promise(resolve => setTimeout(resolve, ms))

// Creates a ProfileScraper.
const scraper = new ProfileScraper()

/**
 * Fetches and process `n` comments from `subreddit`
 * @param subreddit to fetch
 * @param each callback to process the subreddit comment data
 */
function fetchSubredditComments (subreddit, n, queue) {
  // console.debug('worker: fetchSubredditComments fetching', n, 'comments from subreddit', subreddit)

  // Fetches reddit.com/${subreddit}/comments.json?limit=n
  let path = `https://www.reddit.com/r/${subreddit}/comments.json?limit=${n}`

  // console.debug('worker: fetchSubredditComments requesting', path)
  axios.get(path)
    .then(
      /**
       * Async callback to process each comment in the 'politics' subreddit.
       * Fetches comment author's profile data and formats the comment, before queueing it to be sent to Kafka.
       * @param response from axios.get
       */
      (response) => {
        console.info('worker: received', response.data.data.dist, 'comments from', subreddit)

        // Processes each `response.data.data` (comment data) with given callback.
        response.data.data.children.forEach(async (child) => {
          const comment = child.data
          const profile = await scraper.fetchProfile(comment.author)

          if (profile.error) return
          // TODO: Do anything else? (fetchProfile already logs an error.)

          const fullComment = formatComment(profile, comment)

          // Sets is_bot and is_troll to `null` signaling we don't know yet.
          fullComment.is_bot = null
          fullComment.is_troll = null

          // Fetch last ≤20 user comment Reddit ids to this comment (data structure).
          let commentsAfterId = await scraper.fetchRecentComments(profile.name, comment.link_id, comment.created)
          if (NODE_ENV !== 'production') {
            // Moved after the above `await` so it's simultaneous with the next `console.debug`:
            console.info('worker: processing', comment.link_id, comment.created, 'from', profile.name)
          }

          if (commentsAfterId.error) return
          // TODO: Do anything else? (fetchRecentComments already logs an error.)

          // Attaches (≤20) previous comments by the same author to this comment (as a JSON formatted string).
          fullComment.recent_comments = JSON.stringify(commentsAfterId)

          // Marks record as NOT training data.
          fullComment.is_training = false
          if (NODE_ENV !== 'production') {
            // console.debug('worker: fetchSubredditComments cb sending', fullComment)
            console.info(
              'worker: full comment has [ link_id, recent_comments ]',
              [fullComment.link_id, commentsAfterId.map(c => { return { link_id: c.link_id, created: c.created } })]
            )
          }

          // Pushes `comment` task to `queue` (to be sent as message to Kafka).
          queue.push(fullComment)
        })
      }
    )
    .catch((error) => {
      console.error('worker: fetch error!', error)
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
              // TODO: Don't JSON.stringify `message`? See https://www.npmjs.com/package/kafka-node#sendpayloads-cb
            })
            console.info('worker: Kafka producer sent comment', comment.link_id, comment.created_utc, '- offset', result[0].offset)
            cb()
          } catch (error) {
            console.error('worker: Kafka producer submission error!', error)
            cb()
          }
        } else {
          console.info('worker: would produce/send JSON data for', comment.link_id, comment.created_utc, 'comment by', comment.author, 'to Kafka. Q len', kafkaQ.length())
          cb()
        }
      }, 1)

    let interval

    /**
     * Fn (to be ran at interval) for streaming comments as messages to the Kafka topic
     */
    const stream = () => {
      // If the queue is over 500 elements long, the interval stops.
      // TODO: What if the API is unavailable? `fetchSubredditComments` will keep running again and again but `kafkaQ` never grows?
      if (kafkaQ.length() > 500) {
        clearInterval(interval)
        // console.debug('worker interval: suspending...')
        // Re-starts the interval when the last item from queue `kafkaQ` has returned from its worker.
        kafkaQ.drain = () => { // See https://caolan.github.io/async/docs.html#QueueObject
          // console.debug('worker interval: restarting.')
          interval = setInterval(stream, 1000)
        }
      } else {
        // Gets 10 comments from 'politics' subreddit and queues each for processing.
        fetchSubredditComments('politics', 10, kafkaQ)
      }
    }

    // Starts the 1s interval.
    interval = setInterval(stream, 1000)
  } catch (error) {
    console.error('worker error!', error)
  }
}

main()
