/* global require, setTimeout */

const axios = require('axios')

function ProfileScraper () {}

ProfileScraper.prototype.fetchProfile = async function (username) {
  await new Promise(resolve => setTimeout(resolve, 1000))

  try {
    const path = `https://www.reddit.com/${username}/about.json`
    const response = await axios.get(path)
    return response.data.data
  } catch (error) {
    return { error }
  }
}

ProfileScraper.prototype.fetchComments = async function (username, after) {
  await new Promise(resolve => setTimeout(resolve, 1000))

  try {
    console.log(username, after)
    let path = `https://www.reddit.com/${username}/comments.json`

    if (typeof after !== 'undefined') {
      path = `${path}?after=${after}`
    }

    const response = await axios.get(path)
    const { data } = response.data

    const comments = data.children.map(child => {
      return child.data
    })

    if (data.after === null) {
      return comments
    }

    return [].concat(comments, await this.fetchComments(username, data.after))
  } catch (error) {
    console.log(error)
    return { error }
  }
}

ProfileScraper.prototype.scrapeProfile = async function (username) {
  const user = await this.fetchProfile(username)

  if (user.error) {
    return user
  }

  const comments = await this.fetchComments(username)

  return { ...user, comments, isBot: true }
}

module.exports = ProfileScraper
