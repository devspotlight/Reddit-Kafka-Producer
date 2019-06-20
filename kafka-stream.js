/**
 * 3. Fetches n comments from the 'politics' subreddit every 5 seconds, processes and sends them to Kafka.
 * To be ran as a worker.
 */

/* global process, require, setTimeout, setInterval, clearInterval */

const fs = require('fs')

const axios = require('axios')
const queue = require('async.queue')
const Kafka = require('no-kafka')

const ProfileScraper = require('./profile-scraper')
const { formatComment } = require('./format-comment')

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
async function fetchSubredditComments (subreddit, n, queue) {
  // console.debug('worker: fetchSubredditComments fetching', n, 'comments from subreddit', subreddit)

  // Fetches reddit.com/${subreddit}/comments.json?limit=n
  let path = `https://www.reddit.com/r/${subreddit}/comments.json?limit=${n}`

  let response
  try {
    console.debug('worker: fetchSubredditComments requesting', path)
    response = await axios.get(path)
    // /**
    //  * Async callback to process each comment in the 'politics' subreddit.
    //  * Fetches comment author's profile data and formats the comment, before queueing it to be sent to Kafka.
    //  * @param response from axios.get
    //  */
  } catch (ex) {
    console.error('worker: fetch error!', ex)
    return
  }
  // console.debug('worker: response', response)

  console.info('worker: received', response.data.data.dist, 'comments from', subreddit)
  const comments = response.data.data.children.reverse()
  // console.debug('worker: comments', comments)

  console.debug(`worker: children`, comments.map(
    c => { return { name: c.data.name, created_utc: c.data.created_utc } }
  ))

  // Processes each `response.data.data` (comment data).
  comments.forEach(async (child) => {
    // FIXME: This async `forEach` callback messes up the queueing order (`queue.push`).
    const comment = child.data
    const profile = await scraper.fetchProfile(comment.author)

    if (profile.error) return
    // TODO: Do anything else? (fetchProfile already logs an error.)

    // TODO: Use comment.name (t1_ https://www.reddit.com/dev/api#fullnames) instead of comment.link_id (t3_ fullname) !

    const fullComment = formatComment(profile, comment)

    // Sets is_bot and is_troll to `null` signaling we don't know yet.
    fullComment.is_bot = null
    fullComment.is_troll = null

    // Fetch last ≤20 user comment Reddit ids to this comment (data structure).
    let commentsAfterId = await scraper.fetchRecentComments(profile, comment.link_id, comment.created_utc, 20)
    if (commentsAfterId.error) return
    // TODO: Do anything else? (fetchRecentComments already logs an error.)

    if (NODE_ENV !== 'production') {
      console.info('worker: processing', comment.link_id, comment.created_utc, 'by', profile.name, '(with', commentsAfterId.length, 'recent commits)')
    }

    // Attaches (≤20) previous comments by the same author to this comment (as a JSON formatted string).
    fullComment.recent_comments = JSON.stringify(commentsAfterId)

    // Marks record as NOT training data.
    fullComment.is_training = false

    // console.debug('worker: comment', comment)
    // console.debug('worker: commentsAfterId', commentsAfterId)
    // console.debug('worker: fullComment', fullComment)

    // if (NODE_ENV !== 'production') {
    //   // console.debug('worker: fetchSubredditComments cb sending', fullComment)
    //   console.debug(
    //     'worker: full comment has [ link_id, recent_comments ]',
    //     [fullComment.link_id, commentsAfterId.map(c => { return { link_id: c.link_id, created_utc: c.created_utc } })]
    //   )
    // }

    // console.debug('worker: pushing', fullComment.link_id, fullComment.created_utc)
    // Pushes `comment` task to `queue` (to be sent as message to Kafka).
    queue.push(fullComment)
  })
}

/**
 * FIXME: `async` `main` fn just to `await producer.init()`
 * i.e. `producer.init()` and `pool.connect()`
 * @returns {Promise<void>}
 */
async function main () {
  try {
    // Loads env vars for connecting to Kafka.
    const url = process.env.KAFKA_URL
    const cert = process.env.KAFKA_CLIENT_CERT
    const key = process.env.KAFKA_CLIENT_CERT_KEY

    if (NODE_ENV === 'production') {
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
    } else {
      console.info('worker: NOT PROD. Skipping Kafka connection.')
    }

    /* Queue to send comments to the Kafka topic */ // See https://caolan.github.io/async/docs.html#queue
    // FIXME: Defined inside `main` so it can access `producer`
    const kafkaQ = queue(
      /**
       * Async `kafkaQ` worker fn to send each comment to Kafka (via `producer`).
       * TODO https://github.com/oleksiyk/kafka/blob/master/README.ts.md#batching-grouping-produce-requests
       * @param comment Reddit comment JSON data expected
       * @param cb callback invoked when done
       * @returns {Promise<void>}
       */
      async (comment, cb) => {
        // console.debug('worker: producing JSON.stringify of comment', comment.link_id, 'for Kafka')
        // console.debug('worker: comment data', comment)

        // Try sending stringified JSON `comment` to Kafka
        if (NODE_ENV === 'production') {
          try {
            let result = await producer.send({
              topic: 'pearl-20877.reddit-comments', // TODO: Hardcoded topic name
              partition: 0,
              // key: `${comment.link_id}.${comment.created_utc}`, // To use http://kafka.apache.org/documentation.html#compaction
              // TODO: Don't JSON.stringify `message`? See https://www.npmjs.com/package/kafka-node#sendpayloads-cb
              message: { value: JSON.stringify(comment) }
            })
            console.info('worker: Kafka producer sent comment', comment.link_id, comment.created_utc, '- offset', result[0].offset)
            // console.debug('response', result)
          } catch (error) {
            console.error('worker: Kafka producer submission error!', error)
          }
        } else {
          console.info('worker: would produce (Kafka) message with key', `${comment.link_id}.${comment.created_utc} (comment by ${comment.author}). Q len`, kafkaQ.length())
        }
        await wait(500) // pace Kafka producer
        cb()
      },
      1)

    /**
     * Fn (to be ran at interval) for streaming comments as messages to the Kafka topic
     */
    const stream = async () => {
      let n = 10 // comments to fetch per request
      let m = 5 // (times `n` comments) to define Kafka producer capacity

      // Processes and queues `n` comments.
      await fetchSubredditComments('politics', n, kafkaQ)

      await wait(5000) // pace Reddit API requests

      // If Kafka producer queue isn't at capacity, fetch more comments.
      if (kafkaQ.length() < n * m) {
        stream()
      }
    }

    stream()

    // If/When Kafka producer queue is getting empty, restart streaming
    kafkaQ.empty = stream
    //
  } catch (error) {
    console.error('worker error!', error)
  }
}

main()
