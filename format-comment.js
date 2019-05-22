/**
 * Function for formatting JSON response data.
 */

/* global module */

module.exports = {
  /**
   * Builds full comment record by whitelisting it from both profile and comment data.
   * NOTE: `comment.body` is sliced to 256 chars
   * @param profile user data from Reddit
   * @param comment message data from Reddit
   * @returns {{num_reports: *, gilded: *, downs: *, author: *, over_18: *, controversiality: *, body: string, link_id: *, author_verified: *, author_link_karma: *, num_comments: *, score: *, author_comment_karma: *, is_submitter: *, no_follow: *, ups: *, quarantine: *, created_utc: *, banned_by: *}}
   */
  formatComment: function (profile, comment) {
    return {
      banned_by: comment.banned_by,
      no_follow: comment.no_follow,
      link_id: comment.link_id,
      gilded: comment.gilded,
      author: comment.author,
      author_verified: profile.verified,
      author_comment_karma: profile.comment_karma,
      author_link_karma: profile.link_karma,
      num_comments: comment.num_comments,
      created_utc: comment.created_utc,
      score: comment.score,
      over_18: comment.over_18,
      body: comment.body.slice(0, 256).replace(/["|\n|\r\n]/, ''),
      downs: comment.downs,
      is_submitter: comment.is_submitter,
      num_reports: comment.num_reports,
      controversiality: comment.controversiality,
      quarantine: comment.quarantine,
      ups: comment.ups
    }
  }
}
