/**
 * 3. Fetches n comments from the 'politics' subreddit, processes and sends them to Kafka. Repeats.
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
let millis = ms => new Promise(resolve => setTimeout(resolve, ms))

// Creates a ProfileScraper.
const scraper = new ProfileScraper()

// There's no previosly last comment id for the first fetch.
let prevLastComment = false

/**
 * Fetches and process `n` comments from `subreddit`
 * @param subreddit to fetch
 * @param each callback to process the subreddit comment data
 */
async function fetchSubredditComments (subreddit, n, queue) {
  // console.debug('worker: fetch -', n, 'comments from subreddit', subreddit)

  // Fetches reddit.com/${subreddit}/comments.json?limit=n
  let path = `https://www.reddit.com/r/${subreddit}/comments.json?limit=${n}`

  let response
  try {
    // console.debug('worker: fetch -', path)
    response = await axios.get(path)
    // /**
    //  * Async callback to process each comment in the 'politics' subreddit.
    //  * Fetches comment author's profile data and formats the comment, before queueing it to be sent to Kafka.
    //  * @param response from axios.get
    //  */
  } catch (ex) {
    console.error('worker: fetch - error!', ex)
    return
  }
  // console.debug('worker: fetch - received', response.data.data.dist, `comments from r/${subreddit}`)
  // console.debug('worker: fetch - response', response)

  // Extracts and reorgs comments from response data.
  let comments = response.data.data.children.reverse().map(c => c.data)
  // console.debug(`worker: fetch - comments`, comments.map(
  //   c => { return { id: c.id, created_utc: c.created_utc } }
  // ))

  // Checks last comment hasn't been processed yet before continuing.
  const thisLastComment = comments[comments.length - 1].id
  // console.debug('worker: fetch - last comment', this_last_cmt_it)
  if (prevLastComment === thisLastComment) {
    // Otherwise, try again in 1 second.
    console.info('worker: fetch - No new comments. Will try again after 1 sec...')
    await millis(1000)
    console.info('worker: fetch - Trying again after 1 sec!')
    fetchSubredditComments('politics', 10, queue)
  }

  // Removes comments up to `prevLastComment` (if present) – to process only new ones.
  for (let c = 0; c < comments.length; c++) {
    if (comments[c].id === prevLastComment) {
      comments = comments.slice(c + 1)
      break
    }
  }
  console.debug(`worker: fetch - new comments`, comments.map(
    c => { return { id: c.id, created_utc: c.created_utc } }
  ))

  // Updates prevLastComment for next fetch.
  prevLastComment = thisLastComment

  // Processes and queues each (new) comment!
  comments.forEach(async (comment) => {
    // FIXME: This async `forEach` callback messes up the queueing order (`queue.push`).
    const profile = await scraper.fetchProfile(comment.author)

    if (profile.error) return
    // TODO: Do anything else? (fetchProfile already logs an error.)

    // TODO: Use comment.id instead of comment.link_id !

    const fullComment = formatComment(profile, comment)

    // Sets is_bot and is_troll to `null` signaling we don't know yet.
    fullComment.is_bot = null
    fullComment.is_troll = null

    // Fetch last ≤20 user comment Reddit ids to this comment (data structure).
    let commentsAfterId = await scraper.fetchRecentComments(profile, comment.link_id, comment.created_utc, 20)
    if (commentsAfterId.error) return
    // TODO: Do anything else? (fetchRecentComments already logs an error.)

    if (NODE_ENV !== 'production') {
      console.info('worker: fetch - processing', comment.link_id, comment.created_utc, 'by', profile.name, '(with', commentsAfterId.length, 'recent commits)')
    }

    // Attaches (≤20) previous comments by the same author to this comment (as a JSON formatted string).
    fullComment.recent_comments = JSON.stringify(commentsAfterId)

    // Marks record as NOT training data.
    fullComment.is_training = false

    // console.debug('worker: fetch - comment', comment)
    // console.debug('worker: fetch - commentsAfterId', commentsAfterId)
    // console.debug('worker: fetch - fullComment', fullComment)

    // if (NODE_ENV !== 'production') {
    //   // console.debug('worker: fetch - fetchSubredditComments cb sending', fullComment)
    //   console.debug(
    //     'worker: fetch - full comment has [ link_id, recent_comments ]',
    //     [fullComment.link_id, commentsAfterId.map(c => { return { link_id: c.link_id, created_utc: c.created_utc } })]
    //   )
    // }

    // console.debug('worker: fetch - queueing', fullComment.link_id, fullComment.created_utc)
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
        // console.debug('worker: queue - producing JSON.stringify of comment', comment.link_id, 'for Kafka')
        // console.debug('worker: queue - comment data', comment)

        // Try sending stringified JSON `comment` to Kafkaworker: children
        if (NODE_ENV === 'production') {
          try {
            let result = await producer.send({
              topic: 'pearl-20877.reddit-comments', // TODO: Hardcoded topic name
              partition: 0,
              // key: `${comment.link_id}.${comment.created_utc}`, // To use http://kafka.apache.org/documentation.html#compaction
              // TODO: Don't JSON.stringify `message`? See https://www.npmjs.com/package/kafka-node#sendpayloads-cb
              message: { value: JSON.stringify(comment) }
            })
            console.info('worker: queue - Kafka producer sent comment', comment.link_id, comment.created_utc, '- offset', result[0].offset)
            // console.debug('response', result)
          } catch (error) {
            console.error('worker: queue - Kafka producer submission error!', error)
          }
        } else {
          console.info('worker: queue - would produce', (JSON.stringify(comment).length * 2) , 'B (Kafka) message', `${comment.link_id} ${comment.created_utc} (comment by ${comment.author}). Q len`, kafkaQ.length())
        }
        await millis(333) // pace Kafka producer // aprox speed for 64 KB rate limit (w/ avg msg size of 20 KB)
        cb()
      },
      1)

    // If/When Kafka producer queue is getting empty, restarts streaming.
    kafkaQ.empty = () => {
      console.info('worker: queue emptied. Restarting!')
      fetchSubredditComments('politics', 10, kafkaQ)
    }

    // Starts streaming.
    fetchSubredditComments('politics', 10, kafkaQ)
    //
  } catch (error) {
    console.error('worker error!', error)
  }
}

main()
