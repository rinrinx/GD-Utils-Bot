const fs = require('fs')
const path = require('path')
const dayjs = require('dayjs')
const prompts = require('prompts')
const pLimit = require('p-limit')
const axios = require('@viegg/axios')
const { GoogleToken } = require('gtoken')
const handle_exit = require('signal-exit')
const bytes = require('bytes')
const { argv } = require('yargs')

let { PARALLEL_LIMIT, EXCEED_LIMIT } = require('../config')
PARALLEL_LIMIT = argv.l || argv.limit || PARALLEL_LIMIT
EXCEED_LIMIT = EXCEED_LIMIT || 7

const { AUTH, RETRY_LIMIT, TIMEOUT_BASE, TIMEOUT_MAX, LOG_DELAY, PAGE_SIZE, DEFAULT_TARGET } = require('../config')
const { db } = require('../db')
const { make_table, make_tg_table, make_html, summary } = require('./summary')
const { gen_tree_html } = require('./tree')
const { snap2html } = require('./snap2html')

const FILE_EXCEED_MSG = 'The number of files on your team drive has exceeded the limit (400,000), Please move the folder that has not been copied to another team drive, and then run the copy command to resume the transfer'
const FOLDER_TYPE = 'application/vnd.google-apps.folder'
const sleep = ms => new Promise((resolve, reject) => setTimeout(resolve, ms))

const { https_proxy, http_proxy, all_proxy } = process.env
const proxy_url = https_proxy || http_proxy || all_proxy

let axins
if (proxy_url) {
  console.log('Use Proxy：', proxy_url)
  let ProxyAgent
  try {
    ProxyAgent = require('proxy-agent')
  } catch (e) { // run npm i proxy-agent
    ProxyAgent = require('https-proxy-agent')
  }
  axins = axios.create({ httpsAgent: new ProxyAgent(proxy_url) })
} else {
  axins = axios.create({})
}

const SA_LOCATION = argv.sa || 'sa'
const SA_BATCH_SIZE = 1000
const SA_FILES = fs.readdirSync(path.join(__dirname, '..', SA_LOCATION)).filter(v => v.endsWith('.json'))
SA_FILES.flag = 0
let SA_TOKENS = get_sa_batch()

if (is_pm2()) {
  setInterval(() => {
    SA_FILES.flag = 0
    SA_TOKENS = get_sa_batch()
  }, 1000 * 3600 * 2)
}

// https://github.com/Leelow/is-pm2/blob/master/index.js
function is_pm2 () {
  return 'PM2_HOME' in process.env || 'PM2_JSON_PROCESSING' in process.env || 'PM2_CLI' in process.env
}

function get_sa_batch () {
  const new_flag = SA_FILES.flag + SA_BATCH_SIZE
  const files = SA_FILES.slice(SA_FILES.flag, new_flag)
  SA_FILES.flag = new_flag
  return files.map(filename => {
    const gtoken = new GoogleToken({
      keyFile: path.join(__dirname, '..', SA_LOCATION, filename),
      scope: ['https://www.googleapis.com/auth/drive']
    })
    return { gtoken, expires: 0 }
  })
}

handle_exit((code, signal) => {
  if (code === 0 && !is_pm2()) return // normal exit in command line, do nothing
  const records = db.prepare('select id from task where status=?').all('copying')
  records.forEach(v => {
    db.prepare('update task set status=? where id=?').run('interrupt', v.id)
  })
  records.length && console.log(records.length, 'task interrupted')
  db.close()
})

async function save_md5 ({fid, size, not_teamdrive, update, service_account}) {
  let files = await walk_and_save({ fid, not_teamdrive, update, service_account })
  files = files.filter(v => v.mimeType !== FOLDER_TYPE)
  if (typeof size !== 'number') size = bytes.parse(size)
  if (size) files = files.filter(v => v.size >= size)
  let cnt = 0
  files.forEach(file => {
    const {md5Checksum, id} = file
    if (!md5Checksum) return
    const record = db.prepare('SELECT * FROM hash WHERE gid = ?').get(id)
    if (record) return
    db.prepare('INSERT INTO hash (gid, md5) VALUES (?, ?)')
      .run(id, md5Checksum)
    cnt++
  })
  console.log('Added', cnt, 'Md5 records')
}

function get_gid_by_md5 (md5) {
  const records = db.prepare('select * from hash where md5=? and status=?').all(md5, 'normal')
  if (!records.length) return null
  // console.log('got existed md5 record in db:', md5)
  return get_random_element(records).gid
}

async function gen_count_body ({ fid, type, update, service_account, limit, tg }) {
  async function update_info () {
    const info = await walk_and_save({ fid, update, service_account, tg })
    return [info, summary(info)]
  }

  function render_smy (smy, type, unfinished_number) {
    if (!smy) return
    if (['html', 'curl', 'tg'].includes(type)) {
      smy = (typeof smy === 'object') ? smy : JSON.parse(smy)
      const type_func = {
        html: make_html,
        curl: make_table,
        tg: make_tg_table
      }
      let result = type_func[type](smy, limit)
      if (unfinished_number) result += `\nNumber of Folders not read：${unfinished_number}`
      return result
    } else { // Default output json
      return (typeof smy === 'string') ? smy : JSON.stringify(smy)
    }
  }
  const file = await get_info_by_id(fid, service_account)
  if (file && file.mimeType !== FOLDER_TYPE) return render_smy(summary([file]), type)

  let info, smy
  const record = db.prepare('SELECT * FROM gd WHERE fid = ?').get(fid)
  if (!file && !record) {
    throw new Error(`Unable to access the link, please check if the link is valid and SA has the appropriate permissions：https://drive.google.com/drive/folders/${fid}`)
  }
  if (!record || update) {
    [info, smy] = await update_info()
  }
  if (type === 'all') {
    info = info || get_all_by_fid(fid)
    if (!info) { // Explain that the last statistical process was interrupted
      [info] = await update_info()
    }
    return info && JSON.stringify(info)
  }
  if (smy) return render_smy(smy, type)
  if (record && record.summary) return render_smy(record.summary, type)
  info = info || get_all_by_fid(fid)
  if (info) {
    smy = summary(info)
  } else {
    [info, smy] = await update_info()
  }
  return render_smy(smy, type, info.unfinished_number)
}

async function count ({ fid, update, sort, type, output, not_teamdrive, service_account }) {
  sort = (sort || '').toLowerCase()
  type = (type || '').toLowerCase()
  output = (output || '').toLowerCase()
  let out_str
  if (!update) {
    if (!type && !sort && !output) {
      const record = db.prepare('SELECT * FROM gd WHERE fid = ?').get(fid)
      const smy = record && record.summary && JSON.parse(record.summary)
      if (smy) return console.log(make_table(smy))
    }
    const info = get_all_by_fid(fid)
    if (info) {
      console.log('cached data found in local database, cache time：', dayjs(info.mtime).format('YYYY-MM-DD HH:mm:ss'))
      if (type === 'snap') {
        const name = await get_name_by_id(fid, service_account)
        out_str = snap2html({ root: { name, id: fid }, data: info })
      } else {
        out_str = get_out_str({ info, type, sort })
      }
      if (output) return fs.writeFileSync(output, out_str)
      return console.log(out_str)
    }
  }
  const with_modifiedTime = type === 'snap'
  const result = await walk_and_save({ fid, not_teamdrive, update, service_account, with_modifiedTime })
  if (type === 'snap') {
    const name = await get_name_by_id(fid, service_account)
    out_str = snap2html({ root: { name, id: fid }, data: result })
  } else {
    out_str = get_out_str({ info: result, type, sort })
  }
  if (output) {
    fs.writeFileSync(output, out_str)
  } else {
    console.log(out_str)
  }
}

function get_out_str ({ info, type, sort }) {
  const smy = summary(info, sort)
  let out_str
  if (type === 'tree') {
    out_str = gen_tree_html(info)
  } else if (type === 'html') {
    out_str = make_html(smy)
  } else if (type === 'json') {
    out_str = JSON.stringify(smy)
  } else if (type === 'all') {
    out_str = JSON.stringify(info)
  } else {
    out_str = make_table(smy)
  }
  return out_str
}

function get_all_by_fid (fid) {
  const record = db.prepare('SELECT * FROM gd WHERE fid = ?').get(fid)
  if (!record) return null
  const { info, subf } = record
  let result = JSON.parse(info)
  result = result.map(v => {
    v.parent = fid
    return v
  })
  if (!subf) return result
  return recur(result, JSON.parse(subf))

  function recur (result, subf) {
    if (!subf.length) return result
    const arr = subf.map(v => {
      const row = db.prepare('SELECT * FROM gd WHERE fid = ?').get(v)
      if (!row) return null // If the corresponding fid record is not found, it means that the process was interrupted last time or the folder was not read completely
      let info = JSON.parse(row.info)
      info = info.map(vv => {
        vv.parent = v
        return vv
      })
      return { info, subf: JSON.parse(row.subf) }
    })
    if (arr.some(v => v === null)) return null
    const sub_subf = [].concat(...arr.map(v => v.subf).filter(v => v))
    result = result.concat(...arr.map(v => v.info))
    return recur(result, sub_subf)
  }
}

async function walk_and_save ({ fid, not_teamdrive, update, service_account, with_modifiedTime, tg }) {
  let result = []
  const unfinished_folders = []
  const limit = pLimit(PARALLEL_LIMIT)

  if (update) {
    const exists = db.prepare('SELECT fid FROM gd WHERE fid = ?').get(fid)
    exists && db.prepare('UPDATE gd SET summary=? WHERE fid=?').run(null, fid)
  }

  const loop = setInterval(() => {
    const now = dayjs().format('HH:mm:ss')
    const message = `${now} | Copied ${result.length} | Ongoing ${limit.activeCount} | Pending ${limit.pendingCount}`
    print_progress(message)
  }, 1000)

  const tg_loop = tg && setInterval(() => {
    tg({
      obj_count: result.length,
      processing_count: limit.activeCount,
      pending_count: limit.pendingCount
    })
  }, 10 * 1000)

  async function recur (parent) {
    let files, should_save
    if (update) {
      files = await limit(() => ls_folder({ fid: parent, not_teamdrive, service_account, with_modifiedTime }))
      should_save = true
    } else {
      const record = db.prepare('SELECT * FROM gd WHERE fid = ?').get(parent)
      if (record) {
        files = JSON.parse(record.info)
      } else {
        files = await limit(() => ls_folder({ fid: parent, not_teamdrive, service_account, with_modifiedTime }))
        should_save = true
      }
    }
    if (!files) return
    if (files.unfinished) unfinished_folders.push(parent)
    should_save && save_files_to_db(parent, files)
    const folders = files.filter(v => v.mimeType === FOLDER_TYPE)
    files.forEach(v => v.parent = parent)
    result = result.concat(files)
    return Promise.all(folders.map(v => recur(v.id)))
  }
  try {
    await recur(fid)
  } catch (e) {
    console.error(e)
  }
  console.log('\nInfo obtained')
  unfinished_folders.length ? console.log('Unread FolderID：', JSON.stringify(unfinished_folders)) : console.log('All Folders have been read')
  clearInterval(loop)
  if (tg_loop) {
    clearInterval(tg_loop)
    tg({
      obj_count: result.length,
      processing_count: limit.activeCount,
      pending_count: limit.pendingCount
    })
  }
  const smy = unfinished_folders.length ? null : summary(result)
  smy && db.prepare('UPDATE gd SET summary=?, mtime=? WHERE fid=?').run(JSON.stringify(smy), Date.now(), fid)
  result.unfinished_number = unfinished_folders.length
  return result
}

function save_files_to_db (fid, files) {
  // Do not save the folder where the request is not completed, then the next call to get_all_by_id will return null, so call walk_and_save again to try to complete the request for this folder
  if (files.unfinished) return
  let subf = files.filter(v => v.mimeType === FOLDER_TYPE).map(v => v.id)
  subf = subf.length ? JSON.stringify(subf) : null
  const exists = db.prepare('SELECT fid FROM gd WHERE fid = ?').get(fid)
  if (exists) {
    db.prepare('UPDATE gd SET info=?, subf=?, mtime=? WHERE fid=?')
      .run(JSON.stringify(files), subf, Date.now(), fid)
  } else {
    db.prepare('INSERT INTO gd (fid, info, subf, ctime) VALUES (?, ?, ?, ?)')
      .run(fid, JSON.stringify(files), subf, Date.now())
  }
}

async function ls_folder ({ fid, not_teamdrive, service_account, with_modifiedTime }) {
  let files = []
  let pageToken
  const search_all = { includeItemsFromAllDrives: true, supportsAllDrives: true }
  const params = ((fid === 'root') || not_teamdrive) ? {} : search_all
  params.q = `'${fid}' in parents and trashed = false`
  params.orderBy = 'folder,name desc'
  params.fields = 'nextPageToken, files(id, name, mimeType, size, md5Checksum)'
  if (with_modifiedTime) {
    params.fields = 'nextPageToken, files(id, name, mimeType, modifiedTime, size, md5Checksum)'
  }
  params.pageSize = Math.min(PAGE_SIZE, 1000)
  // const use_sa = (fid !== 'root') && (service_account || !not_teamdrive) // Without parameters, use sa by default
  const use_sa = (fid !== 'root') && service_account
  // const headers = await gen_headers(use_sa)
  // For Folders with a large number of subfolders（1ctMwpIaBg8S1lrZDxdynLXJpMsm5guAl），The access_token may have expired before listing
  // Because nextPageToken is needed to get the data of the next page，So you cannot use parallel requests，The test found that each request to obtain 1000 files usually takes more than 20 seconds to complete
  const gtoken = use_sa && (await get_sa_token()).gtoken
  do {
    if (pageToken) params.pageToken = pageToken
    let url = 'https://www.googleapis.com/drive/v3/files'
    url += '?' + params_to_query(params)
    let retry = 0
    let data
    const payload = { timeout: TIMEOUT_BASE }
    while (!data && (retry < RETRY_LIMIT)) {
      const access_token = gtoken ? (await gtoken.getToken()).access_token : (await get_access_token())
      const headers = { authorization: 'Bearer ' + access_token }
      payload.headers = headers
      try {
        data = (await axins.get(url, payload)).data
      } catch (err) {
        handle_error(err)
        retry++
        payload.timeout = Math.min(payload.timeout * 2, TIMEOUT_MAX)
      }
    }
    if (!data) {
      console.error('Folder is not read completely, Parameters:', params)
      files.unfinished = true
      return files
    }
    files = files.concat(data.files)
    argv.sfl && console.log('files.length:', files.length)
    pageToken = data.nextPageToken
  } while (pageToken)

  return files
}

async function gen_headers (use_sa) {
  // use_sa = use_sa && SA_TOKENS.length
  const access_token = use_sa ? (await get_sa_token()).access_token : (await get_access_token())
  return { authorization: 'Bearer ' + access_token }
}

function params_to_query (data) {
  const ret = []
  for (let d in data) {
    ret.push(encodeURIComponent(d) + '=' + encodeURIComponent(data[d]))
  }
  return ret.join('&')
}

async function get_access_token () {
  const { expires, access_token, client_id, client_secret, refresh_token } = AUTH
  if (expires > Date.now()) return access_token

  const url = 'https://www.googleapis.com/oauth2/v4/token'
  const headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
  const config = { headers }
  const params = { client_id, client_secret, refresh_token, grant_type: 'refresh_token' }
  const { data } = await axins.post(url, params_to_query(params), config)
  // console.log('Got new token:', data)
  AUTH.access_token = data.access_token
  AUTH.expires = Date.now() + 1000 * data.expires_in
  return data.access_token
}

// get_sa_token().then(console.log).catch(console.error)
async function get_sa_token () {
  if (!SA_TOKENS.length) SA_TOKENS = get_sa_batch()
  while (SA_TOKENS.length) {
    const tk = get_random_element(SA_TOKENS)
    try {
      return await real_get_sa_token(tk)
    } catch (e) {
      console.warn('SA failed to get access_token：', e.message)
      SA_TOKENS = SA_TOKENS.filter(v => v.gtoken !== tk.gtoken)
      if (!SA_TOKENS.length) SA_TOKENS = get_sa_batch()
    }
  }
  throw new Error('No SA available')
}

async function real_get_sa_token (el) {
  const { value, expires, gtoken } = el
  // The reason for passing out gtoken is that when an account is exhausted, it can be filtered accordingly
  if (Date.now() < expires) return { access_token: value, gtoken }
  const { access_token, expires_in } = await gtoken.getToken({ forceRefresh: true })
  el.value = access_token
  el.expires = Date.now() + 1000 * (expires_in - 60 * 5) // 5 mins passed is taken as Expired
  return { access_token, gtoken }
}

function get_random_element (arr) {
  return arr[~~(arr.length * Math.random())]
}

function validate_fid (fid) {
  if (!fid) return false
  fid = String(fid)
  const whitelist = ['root', 'appDataFolder', 'photos']
  if (whitelist.includes(fid)) return true
  if (fid.length < 10 || fid.length > 100) return false
  const reg = /^[a-zA-Z0-9_-]+$/
  return fid.match(reg)
}

async function create_folder (name, parent, use_sa, limit) {
  let url = `https://www.googleapis.com/drive/v3/files`
  const params = { supportsAllDrives: true }
  url += '?' + params_to_query(params)
  const post_data = {
    name,
    mimeType: FOLDER_TYPE,
    parents: [parent]
  }
  let retry = 0
  let err_message
  while (retry < RETRY_LIMIT) {
    try {
      const headers = await gen_headers(use_sa)
      return (await axins.post(url, post_data, { headers })).data
    } catch (err) {
      err_message = err.message
      retry++
      handle_error(err)
      const data = err && err.response && err.response.data
      const message = data && data.error && data.error.message
      if (message && message.toLowerCase().includes('file limit')) {
        if (limit) limit.clearQueue()
        throw new Error(FILE_EXCEED_MSG)
      }
      console.log('Creating Folder and Retrying：', name, 'No of retries：', retry)
    }
  }
  throw new Error(err_message + ' Folder Name：' + name)
}

async function get_name_by_id (fid, use_sa) {
  const info = await get_info_by_id(fid, use_sa)
  return info ? info.name : fid
}

async function get_info_by_id (fid, use_sa) {
  let url = `https://www.googleapis.com/drive/v3/files/${fid}`
  let params = {
    includeItemsFromAllDrives: true,
    supportsAllDrives: true,
    corpora: 'allDrives',
    fields: 'id, name, size, parents, mimeType, modifiedTime'
  }
  url += '?' + params_to_query(params)
  let retry = 0
  while (retry < RETRY_LIMIT) {
    try {
      const headers = await gen_headers(use_sa)
      const { data } = await axins.get(url, { headers })
      return data
    } catch (e) {
      retry++
      handle_error(e)
    }
  }
  // throw new Error('Unable to access this FolderID：' + fid)
}

async function user_choose () {
  const answer = await prompts({
    type: 'select',
    name: 'value',
    message: 'Do you wish to resume？',
    choices: [
      { title: 'Continue', description: 'Resume the transfer', value: 'continue' },
      { title: 'Restart', description: 'Restart the process', value: 'restart' },
      { title: 'Exit', description: 'Exit', value: 'exit' }
    ],
    initial: 0
  })
  return answer.value
}

async function copy ({ source, target, name, min_size, update, not_teamdrive, service_account, dncnr, is_server }) {
  target = target || DEFAULT_TARGET
  if (!target) throw new Error('Destination ID cannot be empty')

  const file = await get_info_by_id(source, service_account)
  if (!file) return console.error(`Unable to access the link, please check if the link is valid and SA has the appropriate permissions：https://drive.google.com/drive/folders/${source}`)
  if (file && file.mimeType !== FOLDER_TYPE) {
    if (argv.hash_server === 'local') source = get_gid_by_md5(file.md5Checksum)
    return copy_file(source, target, service_account).catch(console.error)
  }

  const record = db.prepare('select id, status from task where source=? and target=?').get(source, target)
  if (record && record.status === 'copying') return console.log('This Task is already running. Force Quit')

  try {
    return await real_copy({ source, target, name, min_size, update, dncnr, not_teamdrive, service_account, is_server })
  } catch (err) {
    console.error('Error copying folder', err)
    const record = db.prepare('select id, status from task where source=? and target=?').get(source, target)
    if (record) db.prepare('update task set status=? where id=?').run('error', record.id)
  }
}

// To be resolved: If the user manually interrupts the process with ctrl+c, the request that has been issued will not be recorded in the local database even if it is completed, so duplicate files (folders) may be generated
async function real_copy ({ source, target, name, min_size, update, dncnr, not_teamdrive, service_account, is_server }) {
  async function get_new_root () {
    if (dncnr) return { id: target }
    if (name) {
      return create_folder(name, target, service_account)
    } else {
      const file = await get_info_by_id(source, service_account)
      if (!file) throw new Error(`Unable to access the link, please check if the link is valid and SA has the appropriate permissions：https://drive.google.com/drive/folders/${source}`)
      return create_folder(file.name, target, service_account)
    }
  }

  const record = db.prepare('select * from task where source=? and target=?').get(source, target)
  if (record) {
    const copied = db.prepare('select fileid from copied where taskid=?').all(record.id).map(v => v.fileid)
    const choice = (is_server || argv.yes) ? 'continue' : await user_choose()
    if (choice === 'exit') {
      return console.log('exit the program')
    } else if (choice === 'continue') {
      let { mapping } = record
      const old_mapping = {}
      const copied_ids = {}
      copied.forEach(id => copied_ids[id] = true)
      mapping = mapping.trim().split('\n').map(line => line.split(' '))
      const root = mapping[0][1]
      mapping.forEach(arr => old_mapping[arr[0]] = arr[1])
      db.prepare('update task set status=? where id=?').run('copying', record.id)
      const arr = await walk_and_save({ fid: source, update, not_teamdrive, service_account })
      let files = arr.filter(v => v.mimeType !== FOLDER_TYPE).filter(v => !copied_ids[v.id])
      if (min_size) files = files.filter(v => v.size >= min_size)
      const folders = arr.filter(v => v.mimeType === FOLDER_TYPE)
      const all_mapping = await create_folders({
        old_mapping,
        source,
        folders,
        service_account,
        root,
        task_id: record.id
      })
      await copy_files({ files, service_account, root, mapping: all_mapping, task_id: record.id })
      db.prepare('update task set status=?, ftime=? where id=?').run('finished', Date.now(), record.id)
      return { id: root, task_id: record.id }
    } else if (choice === 'restart') {
      const new_root = await get_new_root()
      const root_mapping = source + ' ' + new_root.id + '\n'
      db.prepare('update task set status=?, mapping=? where id=?').run('copying', root_mapping, record.id)
      db.prepare('delete from copied where taskid=?').run(record.id)
      // const arr = await walk_and_save({ fid: source, update: true, not_teamdrive, service_account })
      const arr = await walk_and_save({ fid: source, update, not_teamdrive, service_account })

      let files = arr.filter(v => v.mimeType !== FOLDER_TYPE)
      if (min_size) files = files.filter(v => v.size >= min_size)
      const folders = arr.filter(v => v.mimeType === FOLDER_TYPE)
      console.log('Number of folders to be copied：', folders.length)
      console.log('Number of files to be copied：', files.length)
      const mapping = await create_folders({
        source,
        folders,
        service_account,
        root: new_root.id,
        task_id: record.id
      })
      await copy_files({ files, mapping, service_account, root: new_root.id, task_id: record.id })
      db.prepare('update task set status=?, ftime=? where id=?').run('finished', Date.now(), record.id)
      return { id: new_root.id, task_id: record.id }
    } else {
      // ctrl+c Exit
      return console.log('Exit')
    }
  } else {
    const new_root = await get_new_root()
    const root_mapping = source + ' ' + new_root.id + '\n'
    const { lastInsertRowid } = db.prepare('insert into task (source, target, status, mapping, ctime) values (?, ?, ?, ?, ?)').run(source, target, 'copying', root_mapping, Date.now())
    const arr = await walk_and_save({ fid: source, update, not_teamdrive, service_account })
    let files = arr.filter(v => v.mimeType !== FOLDER_TYPE)
    if (min_size) files = files.filter(v => v.size >= min_size)
    const folders = arr.filter(v => v.mimeType === FOLDER_TYPE)
    console.log('Number of folders to be copied：', folders.length)
    console.log('Number of files to be copied：', files.length)
    const mapping = await create_folders({
      source,
      folders,
      service_account,
      root: new_root.id,
      task_id: lastInsertRowid
    })
    await copy_files({ files, mapping, service_account, root: new_root.id, task_id: lastInsertRowid })
    db.prepare('update task set status=?, ftime=? where id=?').run('finished', Date.now(), lastInsertRowid)
    return { id: new_root.id, task_id: lastInsertRowid }
  }
}

async function copy_files ({ files, mapping, service_account, root, task_id }) {
  if (!files.length) return
  console.log('\nStarted copying files, total：', files.length)

  const loop = setInterval(() => {
    const now = dayjs().format('HH:mm:ss')
    const message = `${now} | Number of files copied ${count} | ongoing ${concurrency} | Number of Files Pending ${files.length}`
    print_progress(message)
  }, 1000)

  let count = 0
  let concurrency = 0
  let err
  do {
    if (err) {
      clearInterval(loop)
      files = null
      throw err
    }
    if (concurrency >= PARALLEL_LIMIT) {
      await sleep(100)
      continue
    }
    const file = files.shift()
    if (!file) {
      await sleep(1000)
      continue
    }
    concurrency++
    let { id, parent, md5Checksum } = file
    if (argv.hash_server === 'local') id = get_gid_by_md5(md5Checksum) || id
    const target = mapping[parent] || root
    const use_sa = (id !== file.id) ? true : service_account //If the same md5 record is found in the local database, use sa copy
    copy_file(id, target, use_sa, null, task_id).then(new_file => {
      if (new_file) {
        count++
        db.prepare('INSERT INTO copied (taskid, fileid) VALUES (?, ?)').run(task_id, file.id)
      }
    }).catch(e => {
      err = e
    }).finally(() => {
      concurrency--
    })
  } while (concurrency || files.length)
  clearInterval(loop)
  if (err) throw err
  // const limit = pLimit(PARALLEL_LIMIT)
  // let count = 0
  // const loop = setInterval(() => {
  //   const now = dayjs().format('HH:mm:ss')
  //   const {activeCount, pendingCount} = limit
  //   const message = `${now} | Number of files copied ${count} | Ongoing ${activeCount} | Pending ${pendingCount}`
  //   print_progress(message)
  // }, 1000)
  // May cause excessive memory usage and be forced to exit by node
  // return Promise.all(files.map(async file => {
  //   const { id, parent } = file
  //   const target = mapping[parent] || root
  //   const new_file = await limit(() => copy_file(id, target, service_account, limit, task_id))
  //   if (new_file) {
  //     count++
  //     db.prepare('INSERT INTO copied (taskid, fileid) VALUES (?, ?)').run(task_id, id)
  //   }
  // })).finally(() => clearInterval(loop))
}

async function copy_file (id, parent, use_sa, limit, task_id) {
  let url = `https://www.googleapis.com/drive/v3/files/${id}/copy`
  let params = { supportsAllDrives: true }
  url += '?' + params_to_query(params)
  const config = {}
  let retry = 0
  while (retry < RETRY_LIMIT) {
    let gtoken
    if (use_sa) {
      const temp = await get_sa_token()
      gtoken = temp.gtoken
      config.headers = { authorization: 'Bearer ' + temp.access_token }
    } else {
      config.headers = await gen_headers()
    }
    try {
      const { data } = await axins.post(url, { parents: [parent] }, config)
      if (gtoken) gtoken.exceed_count = 0
      return data
    } catch (err) {
      retry++
      handle_error(err)
      const data = err && err.response && err.response.data
      const message = data && data.error && data.error.message
      if (message && message.toLowerCase().includes('file limit')) {
        if (limit) limit.clearQueue()
        if (task_id) db.prepare('update task set status=? where id=?').run('error', task_id)
        throw new Error(FILE_EXCEED_MSG)
      }
      if (!use_sa && message && message.toLowerCase().includes('rate limit')) {
        throw new Error('Personal Drive Limit：' + message)
      }
      // if (use_sa && message && message.toLowerCase().includes('user rate limit')) {
      //  if (retry >= RETRY_LIMIT) throw new Error(`This resource triggers a userRateLimitExceeded error for ${EXCEED_LIMIT} consecutive times and stops copying`)
      //  if (gtoken.exceed_count >= EXCEED_LIMIT) {
      //    SA_TOKENS = SA_TOKENS.filter(v => v.gtoken !== gtoken)
      //   if (!SA_TOKENS.length) SA_TOKENS = get_sa_batch()
      //  console.log(`This account has triggered the daily usage limit${EXCEED_LIMIT} consecutive times, the remaining amount of SA available in this batch：`, SA_TOKENS.length)
      // } else {
          // console.log('This account triggers its daily usage limit and has been marked. If the next request is normal, it will be unmarked, otherwise the SA will be removed')
         // if (gtoken.exceed_count) {
          //  gtoken.exceed_count++
         // } else {
           // gtoken.exceed_count = 1
         // }
       // }
     // }
    }
  }
  if (use_sa && !SA_TOKENS.length) {
    if (limit) limit.clearQueue()
    if (task_id) db.prepare('update task set status=? where id=?').run('error', task_id)
    throw new Error('All SA are exhausted')
  } else {
    console.warn('File creation failed，Fileid: ' + id)
  }
}

async function create_folders ({ source, old_mapping, folders, root, task_id, service_account }) {
  if (argv.dncf) return {} // do not copy folders
  if (!Array.isArray(folders)) throw new Error('folders must be Array:' + folders)
  const mapping = old_mapping || {}
  mapping[source] = root
  if (!folders.length) return mapping

  const missed_folders = folders.filter(v => !mapping[v.id])
  console.log('Start copying folders, total：', missed_folders.length)
  const limit = pLimit(PARALLEL_LIMIT)
  let count = 0
  let same_levels = folders.filter(v => v.parent === folders[0].parent)

  const loop = setInterval(() => {
    const now = dayjs().format('HH:mm:ss')
    const message = `${now} | Folders Created ${count} | Ongoing ${limit.activeCount} | Pending ${limit.pendingCount}`
    print_progress(message)
  }, 1000)

  while (same_levels.length) {
    const same_levels_missed = same_levels.filter(v => !mapping[v.id])
    await Promise.all(same_levels_missed.map(async v => {
      try {
        const { name, id, parent } = v
        const target = mapping[parent] || root
        const new_folder = await limit(() => create_folder(name, target, service_account, limit))
        count++
        mapping[id] = new_folder.id
        const mapping_record = id + ' ' + new_folder.id + '\n'
        db.prepare('update task set mapping = mapping || ? where id=?').run(mapping_record, task_id)
      } catch (e) {
        if (e.message === FILE_EXCEED_MSG) {
          clearInterval(loop)
          throw new Error(FILE_EXCEED_MSG)
        }
        console.error('Error creating Folder:', e.message)
      }
    }))
    // folders = folders.filter(v => !mapping[v.id])
    same_levels = [].concat(...same_levels.map(v => folders.filter(vv => vv.parent === v.id)))
  }

  clearInterval(loop)
  return mapping
}

function find_dupe (arr) {
  const files = arr.filter(v => v.mimeType !== FOLDER_TYPE)
  const folders = arr.filter(v => v.mimeType === FOLDER_TYPE)
  const exists = {}
  const dupe_files = []
  const dupe_folder_keys = {}
  for (const folder of folders) {
    const { parent, name } = folder
    const key = parent + '|' + name
    if (exists[key]) {
      dupe_folder_keys[key] = true
    } else {
      exists[key] = true
    }
  }
  const dupe_empty_folders = folders.filter(folder => {
    const { parent, name } = folder
    const key = parent + '|' + name
    return dupe_folder_keys[key]
  }).filter(folder => {
    const has_child = arr.some(v => v.parent === folder.id)
    return !has_child
  })
  for (const file of files) {
    const { md5Checksum, parent, name, size } = file
    // Determining Duplicates based on file location and md5 value
    const key = parent + '|' + md5Checksum
    if (exists[key]) {
      dupe_files.push(file)
    } else {
      exists[key] = true
    }
  }
  return dupe_files.concat(dupe_empty_folders)
}

async function confirm_dedupe ({ file_number, folder_number }) {
  const answer = await prompts({
    type: 'select',
    name: 'value',
    message: `Duplicate files detected ${file_number}，Empty Folders detected${folder_number}，Delete them？`,
    choices: [
      { title: 'Yes', description: 'confirm deletion', value: 'yes' },
      { title: 'No', description: 'Donot delete', value: 'no' }
    ],
    initial: 0
  })
  return answer.value
}

// Need sa to be the manager of the Teamdrive where the source folder is located
async function mv_file ({ fid, new_parent, service_account }) {
  const file = await get_info_by_id(fid, service_account)
  if (!file) return
  const removeParents = file.parents[0]
  let url = `https://www.googleapis.com/drive/v3/files/${fid}`
  const params = {
    removeParents,
    supportsAllDrives: true,
    addParents: new_parent
  }
  url += '?' + params_to_query(params)
  const headers = await gen_headers(service_account)
  return axins.patch(url, {}, { headers })
}

// To move files or folders to the recycle bin, SA should be content manager or above
async function trash_file ({ fid, service_account }) {
  const url = `https://www.googleapis.com/drive/v3/files/${fid}?supportsAllDrives=true`
  const headers = await gen_headers(service_account)
  return axins.patch(url, { trashed: true }, { headers })
}

// Delete files or folders directly without entering the recycle bin, requires SA as manager
async function rm_file ({ fid, service_account }) {
  const headers = await gen_headers(service_account)
  let retry = 0
  const url = `https://www.googleapis.com/drive/v3/files/${fid}?supportsAllDrives=true`
  while (retry < RETRY_LIMIT) {
    try {
      return await axins.delete(url, { headers })
    } catch (err) {
      retry++
      handle_error(err)
      console.log('retrying to Delete, retry count', retry)
    }
  }
}

async function dedupe ({ fid, update, service_account, yes }) {
  let arr
  if (!update) {
    const info = get_all_by_fid(fid)
    if (info) {
      console.log('Locally cached data Found, cache time：', dayjs(info.mtime).format('YYYY-MM-DD HH:mm:ss'))
      arr = info
    }
  }
  arr = arr || await walk_and_save({ fid, update, service_account })
  const dupes = find_dupe(arr)
  const folder_number = dupes.filter(v => v.mimeType === FOLDER_TYPE).length
  const file_number = dupes.length - folder_number
  const choice = yes || await confirm_dedupe({ file_number, folder_number })
  if (choice === 'no') {
    return console.log('Exit')
  } else if (!choice) {
    return // ctrl+c
  }
  const limit = pLimit(PARALLEL_LIMIT)
  let folder_count = 0
  let file_count = 0
  await Promise.all(dupes.map(async v => {
    try {
      await limit(() => trash_file({ fid: v.id, service_account }))
      if (v.mimeType === FOLDER_TYPE) {
        console.log('Folder successfully deleted', v.name)
        folder_count++
      } else {
        console.log('File successfully deleted', v.name)
        file_count++
      }
    } catch (e) {
      console.log('Failed to delete', v)
      handle_error(e)
    }
  }))
  return { file_count, folder_count }
}

function handle_error (err) {
  const data = err && err.response && err.response.data
  if (data) {
    const message = data.error && data.error.message
    if (message && message.toLowerCase().includes('rate limit') && !argv.verbose) return
    console.error(JSON.stringify(data))
  } else {
    if (!err.message.includes('timeout') || argv.verbose) console.error(err.message)
  }
}

function print_progress (msg) {
  if (process.stdout.cursorTo) {
    process.stdout.cursorTo(0)
    process.stdout.write(msg + ' ')
  } else {
    console.log(msg)
  }
}

module.exports = { ls_folder, count, validate_fid, copy, dedupe, copy_file, gen_count_body, real_copy, get_name_by_id, get_info_by_id, get_access_token, get_sa_token, walk_and_save, save_md5}
