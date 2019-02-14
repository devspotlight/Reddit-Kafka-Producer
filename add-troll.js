/* global process, require, setTimeout */

const fs = require('fs')
const readline = require('readline')

const { Pool } = require('pg')
const Cursor = require('pg-cursor')
const queue = require('async.queue')

require('dotenv').config()

async function main () {
  const trolls = []

  const pool = new Pool()
  const client = await pool.connect()

  const updateQuery = `update comments set is_troll = true, is_bot = false where id = $1`

  const dbQ = queue(async ({ id }, cb) => {
    console.log('updating', id)
    await pool.query(updateQuery, [ id ])
    cb()
  })

  const lines = readline.createInterface({
    input: fs.createReadStream('bots.csv')
  })

  lines.on('line', line => {
    const fields = line.split(',')
    const username = fields[0]
    const isTroll = fields[2]

    const tmpAuthor = username.split('/')
    const author = tmpAuthor[tmpAuthor.length - 1]

    if (isTroll === 'TRUE') {
      trolls.push(author)
    }
  })

  lines.on('close', () => {
    const cursor = client.query(new Cursor('select * from comments'))

    function loop () {
      cursor.read(10, (err, rows) => {
        if (err) throw err

        if (rows.length === 0) {
          dbQ.drain = () => {}
          console.log('done')
          client.release()
          return
        }

        rows.forEach(row => {
          const { id, author } = row
          console.log(`checking ${id}`)

          if (trolls.indexOf(author) > -1) {
            dbQ.push({ id })
          }
        })

        if (dbQ.length() === 0) {
          loop()
        }
      })
    }

    dbQ.drain = loop
    loop()
  })
}

main()
