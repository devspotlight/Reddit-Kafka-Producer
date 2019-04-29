/**
 * Class for accessing data from the Reddit API.
 */

/* global require, setTimeout */

const axios = require('axios')

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
  // await new Promise(resolve => setTimeout(resolve, 100))

  try {
    const path = `https://www.reddit.com/${username}/about.json`
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
    console.debug('ProfileScraper.fetchComments for', username, 'after', after)
    let path = `https://www.reddit.com/${username}/comments.json`

    if (typeof after !== 'undefined') {
      path += `?after=${after}`
    }

    const response = await axios.get(path)
    // data = response.data.data
    const { data } = response.data

    // Restructures `data.children` sub-object.
    const comments = data.children.map(child => {
      return child.data
    })
    // const comments = data.children.map(child => child.data)

    // Returns the comments when there's no more ones left to fetch.
    if (data.after === null) {
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
 * @returns {Promise<*>} object with user profile, comments, `isBot`, and `isTroll
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

  return { ...user, comments, isBot, isTroll }
}

module.exports = ProfileScraper
