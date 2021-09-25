const Router = require('@koa/router')

const { db } = require('../db')
const { validate_fid, gen_count_body } = require('./gd')
const { send_count, send_help, send_choice, send_task_info, sm, extract_fid, extract_from_text, reply_cb_query, tg_copy, send_all_tasks, send_bm_help, get_target_by_alias, send_all_bookmarks, set_bookmark, unset_bookmark, clear_tasks, send_task_help, rm_task } = require('./tg')

const { AUTH, ROUTER_PASSKEY, TG_IPLIST } = require('../config')
const { tg_whitelist } = AUTH

const COPYING_FIDS = {}
const counting = {}
const router = new Router()

function is_pm2 () {
  return 'PM2_HOME' in process.env || 'PM2_JSON_PROCESSING' in process.env || 'PM2_CLI' in process.env
}

function is_int (n) {
  return n === parseInt(n)
}

router.get('/gutils/api/gdurl/count', async ctx => {
  if (!ROUTER_PASSKEY) return ctx.body = 'gd-utils Successfully started'
  const { query, headers } = ctx.request
  let { fid, type, update, passkey } = query
  if (passkey !== ROUTER_PASSKEY) return ctx.body = 'invalid passkey'
  if (!validate_fid(fid)) throw new Error('Invalid FolderID')

  let ua = headers['user-agent'] || ''
  ua = ua.toLowerCase()
  type = (type || '').toLowerCase()
  // todo type=tree
  if (!type) {
    if (ua.includes('curl')) {
      type = 'curl'
    } else if (ua.includes('mozilla')) {
      type = 'html'
    } else {
      type = 'json'
    }
  }
  if (type === 'html') {
    ctx.set('Content-Type', 'text/html; charset=utf-8')
  } else if (['json', 'all'].includes(type)) {
    ctx.set('Content-Type', 'application/json; charset=UTF-8')
  }
  ctx.body = await gen_count_body({ fid, type, update, service_account: true })
})

router.post('/gutils/api/gdurl/tgbot', async ctx => {
  const { body } = ctx.request
  console.log('ctx.ip', ctx.ip) // You can only allow the ip of the tg server
  console.log('tg message:', JSON.stringify(body, null, '  '))
  if (TG_IPLIST && !TG_IPLIST.includes(ctx.ip)) return ctx.body = 'invalid ip'
  ctx.body = '' // Release the connection early
  const message = body.message || body.edited_message
  const message_str = JSON.stringify(message)

  const { callback_query } = body
  if (callback_query) {
    const { id, message, data } = callback_query
    const chat_id = callback_query.from.id
    const [action, fid, target] = data.split(' ').filter(v => v)
    if (action === 'count') {
      if (counting[fid]) return sm({ chat_id, text: fid + ' Counting, please wait a moment' })
      counting[fid] = true
      send_count({ fid, chat_id }).catch(err => {
        console.error(err)
        sm({ chat_id, text: fid + ' Stats Failed：' + err.message })
      }).finally(() => {
        delete counting[fid]
      })
    } else if (action === 'copy') {
      if (COPYING_FIDS[fid + target]) return sm({ chat_id, text: 'Processing copy command with the same source and destination' })
      COPYING_FIDS[fid + target] = true
      tg_copy({ fid, target: get_target_by_alias(target), chat_id }).then(task_id => {
        is_int(task_id) && sm({ chat_id, text: `Clone Started For Task ID: ${task_id}\nType /task ${task_id} to check the progress` })
      }).finally(() => COPYING_FIDS[fid + target] = false)
    } else if (action === 'update') {
      if (counting[fid]) return sm({ chat_id, text: fid + ' Counting, please wait a moment' })
      counting[fid] = true
      send_count({ fid, chat_id, update: true }).catch(err => {
        console.error(err)
        sm({ chat_id, text: fid + ' Stats Failed：' + err.message })
      }).finally(() => {
        delete counting[fid]
      })
    } else if (action === 'clear_button') {
      const { message_id, text } = message || {}
      if (message_id) sm({ chat_id, message_id, text, parse_mode: 'HTML' }, 'editMessageText')
    }
    return reply_cb_query({ id, data }).catch(console.error)
  }

  const chat_id = message && message.chat && message.chat.id
  const text = (message && message.text && message.text.trim()) || ''
  let username = message && message.from && message.from.username
  username = username && String(username).toLowerCase()
  let user_id = message && message.from && message.from.id
  user_id = user_id && String(user_id).toLowerCase()
  if (!chat_id || !tg_whitelist.some(v => {
    v = String(v).toLowerCase()
    return v === username || v === user_id
  })) {
    chat_id && sm({ chat_id, text: 'You are not supposed to Message me you idiot, go back to the hole you came from' })
    return console.warn('Received a request from a non-whitelisted user')
  }

  const fid = extract_fid(text) || extract_from_text(text) || extract_from_text(message_str)
  const no_fid_commands = ['/task', '/help', '/bm', '/reload']
  if (!no_fid_commands.some(cmd => text.startsWith(cmd)) && !validate_fid(fid)) {
    return sm({ chat_id, text: 'Folder ID is invalid or not accessible' })
  }
  if (text.startsWith('/help')) return send_help(chat_id)
  if (text.startsWith('/reload')) {
    if (!is_pm2()) return sm({ chat_id, text: 'Process is not a pm2 daemon，Do not restart' })
    sm({ chat_id, text: 'Restart' }).then(() => process.exit())
  } else if (text.startsWith('/bm')) {
    const [cmd, action, alias, target] = text.split(' ').map(v => v.trim()).filter(v => v)
    if (!action) return send_all_bookmarks(chat_id)
    if (action === 'set') {
      if (!alias || !target) return sm({ chat_id, text: 'Name and Destination FolderID cannot be empty ' })
      if (alias.length > 24) return sm({ chat_id, text: 'Name Shouldnt be more than 24 Letters in Length' })
      if (!validate_fid(target)) return sm({ chat_id, text: 'Incorrect Destination FolderID' })
      set_bookmark({ chat_id, alias, target })
    } else if (action === 'unset') {
      if (!alias) return sm({ chat_id, text: 'Name Cannot be empty' })
      unset_bookmark({ chat_id, alias })
    } else {
      send_bm_help(chat_id)
    }
  } else if (text.startsWith('/count')) {
    if (counting[fid]) return sm({ chat_id, text: fid + ' Counting, please wait a moment' })
    try {
      counting[fid] = true
      const update = text.endsWith(' -u')
      await send_count({ fid, chat_id, update })
    } catch (err) {
      console.error(err)
      sm({ chat_id, text: fid + ' Stats Failed：' + err.message })
    } finally {
      delete counting[fid]
    }
  } else if (text.startsWith('/copy')) {
    let target = text.replace('/copy', '').replace(' -u', '').trim().split(' ').map(v => v.trim()).filter(v => v)[1]
    target = get_target_by_alias(target) || target
    if (target && !validate_fid(target)) return sm({ chat_id, text: `Destination FolderID ${target} is Invalid` })
    if (COPYING_FIDS[fid + target]) return sm({ chat_id, text: 'Processing copy command with the same source and destination' })
    COPYING_FIDS[fid + target] = true
    const update = text.endsWith(' -u')
    tg_copy({ fid, target, chat_id, update }).then(task_id => {
      is_int(task_id) && sm({ chat_id, text: `Clone Started For Task ID: ${task_id}\nType /task ${task_id} to check the progress` })
    }).finally(() => COPYING_FIDS[fid + target] = false)
  } else if (text.startsWith('/task')) {
    let task_id = text.replace('/task', '').trim()
    if (task_id === 'all') {
      return send_all_tasks(chat_id)
    } else if (task_id === 'clear') {
      return clear_tasks(chat_id)
    } else if (task_id === '-h') {
      return send_task_help(chat_id)
    } else if (task_id.startsWith('rm')) {
      task_id = task_id.replace('rm', '')
      task_id = parseInt(task_id)
      if (!task_id) return send_task_help(chat_id)
      return rm_task({ task_id, chat_id })
    }
    task_id = parseInt(task_id)
    if (!task_id) {
      const running_tasks = db.prepare('select id from task where status=?').all('copying')
      if (!running_tasks.length) return sm({ chat_id, text: 'There are currently no running tasks' })
      return running_tasks.forEach(v => send_task_info({ chat_id, task_id: v.id }).catch(console.error))
    }
    send_task_info({ task_id, chat_id }).catch(console.error)
  } else if (message_str.includes('drive.google.com/') || validate_fid(text)) {
    return send_choice({ fid: fid || text, chat_id })
  } else {
    sm({ chat_id, text: 'This command is not currently supported' })
  }
})

module.exports = router
