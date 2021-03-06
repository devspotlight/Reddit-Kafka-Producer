/**
 * Class for accessing data from the Reddit API.
 */

/* global require, setTimeout */

const axios = require('axios')

const { formatComment } = require('./format-comment')

/**
 * ProfileScraper class
 * @constructor
 */
function ProfileScraper () {}

/**
 * Async fn that gets a Reddit user profile data.
 * @param username to fetch profile for
 * @returns {Promise<*>} user data object
 */
ProfileScraper.prototype.fetchProfile = async function (username) {
  // console.debug('fetchProfile:', username)
  try {
    const path = `https://www.reddit.com/user/${username}/about.json`
    const response = await axios.get(path)
    return response.data.data
  } catch (error) {
    console.error('ProfileScraper.fetchProfile: error!', error)
    return { error }
  }
}

/**
 * Async method that gets all the Reddit comments (data) from a user.
 * @param username to fetch comments for
 * @param after last comment id (https://www.reddit.com/dev/api#fullnames) fetched (optional)
 * @returns {Promise<*>} user comments array
 */
ProfileScraper.prototype.fetchComments = async function (username, after) {
  // Waits 500 ms so reddit.com doesn't block us.
  await new Promise(resolve => setTimeout(resolve, 500))

  // Fetches reddit.com/${username}/comments.json (https://www.reddit.com/dev/api#GET_user_{username}_{where} ?)
  try {
    console.debug('ProfileScraper.fetchComments: for', username, 'after', after)
    let path = `https://www.reddit.com/user/${username}/comments.json`

    if (typeof after !== 'undefined') {
      path += `?after=${after}`
    }

    const response = await axios.get(path)
    // data = response.data.data
    const { data } = response.data

    // Restructures `data.children` sub-object.
    const comments = data.children.map(child => child.data)

    // Returns the comments when there's no more ones left to fetch.
    if (data.after === null) {
      console.debug('ProfileScraper.fetchComments: (', username, ') got', comments.length)
      return comments
    }

    // Recursively get remaining comments in subsequent HTTP calls to the API otherwise.
    return [].concat(comments, await this.fetchComments(username, data.after))
  } catch (error) {
    console.error('ProfileScraper.fetchComments: error!', error)
    return { error }
  }
}

/**
 * Async method that calls `this.fetchProfile` and `this.fetchComments` for a user
 * @param username to fetch profile and comments for
 * @param isBot is returned back as is in an object
 * @param isTroll is returned back as is in an object
 * @returns {Promise<*>} object with {<user profile data fields>, <comments>, `is_bot`, and `is_troll`}
 */
ProfileScraper.prototype.scrapeProfile = async function (username, isBot, isTroll) {
  const user = await this.fetchProfile(username)

  // Makes `user.is_suspended` into a `user.error`
  if (user.is_suspended) {
    user.error = 'suspended'
  }

  // Returns just the `user` (without comments, etc) if there's a `user.error`
  if (user.error) {
    return user
  }

  const comments = await this.fetchComments(username)

  return { ...user, comments, is_bot: isBot, is_troll: isTroll }
}

/**
 * Async fn that tries to get the `n` most recent comments of `profile.name` previous to `linkId`/`created_utc` combo
 * (Only looks in ≤25 latest comments by `profile.name`.)
 * @param profile to fetch recent comments for
 * @param linkId comment.link_id looked for among recent comments by `profile.name`
 * @param createdUtc timestamp looked for among recent comments by `profile.name`
 * @param n Number of recent comments. Default 20. Range [1-24]
 * @returns {Promise<{error: string}|{error: *}|*>} recent comments after `linkId`/`createdUtc`, formatted with `formatComment`
 */
ProfileScraper.prototype.fetchRecentComments = async function (profile, linkId, createdUtc, n = 20) {
  if (Number.isNaN(n) || n < 1 || n > 24) return { error: '`n` param must be in range [1-24]' }
  try {
    const path = `https://www.reddit.com/user/${profile.name}/comments.json?limit=25` // 25 is the default atm but JIC
    const response = await axios.get(path)
    // console.debug('fetchRecentComments: params profile.name link_id createdUtc -', profile.name, linkId, createdUtc) // NOTE: Printed after above `await`
    // console.debug('ProfileScraper.fetchRecentComments: response.data.data.children', response.data.data.children)
    const comments = response.data.data.children.map(child => child.data)
    // console.debug('ProfileScraper.fetchRecentComments:', comments.length, 'comment link_ids', comments.map(c => { return { link_id: c.link_id, created_utc: c.created_utc } }))

    // Tries to find `linkId` among the latest `profile.name` comments.
    let previousTo = -1
    for (let i = 0; i < comments.length; i++) {
      if (comments[i].link_id === linkId && comments[i].created_utc === createdUtc) {
        previousTo = i
        break
      }
    }
    // console.debug('ProfileScraper.fetchRecentComments: found linkId in position', previousTo)
    // Makes sure to return only comments BEFORE linkId (if found) –"after" in the array order– and UP TO `n`.
    let commentsAfterId =
      comments.slice(previousTo + 1, previousTo + 1 + n).map(comment => formatComment(profile, comment))

    // console.debug('ProfileScraper.fetchRecentComments:', commentsAfterId.length, 'comment link_ids AfterId', linkId, createdUtc, 'link_ids', commentsAfterId.map(c => { return { link_id: c.link_id, created_utc: c.created_utc } }))
    return commentsAfterId
  } catch (error) {
    console.error('ProfileScraper.fetchRecentComments error!', error)
    return { error }
  }
}

module.exports = ProfileScraper
