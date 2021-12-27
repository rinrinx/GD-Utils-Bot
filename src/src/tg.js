const Table = require('cli-table3')
const dayjs = require('dayjs')
const axios = require('@viegg/axios')
const HttpsProxyAgent = require('https-proxy-agent')

const { db } = require('../db')
const { gen_count_body, validate_fid, real_copy, get_name_by_id, get_info_by_id, copy_file } = require('./gd')
const { AUTH, DEFAULT_TARGET, USE_PERSONAL_AUTH } = require('../config')
const { tg_token } = AUTH
const gen_link = (fid, text) => `<a href="https://drive.google.com/drive/folders/${fid}">${text || fid}</a>`

if (!tg_token) throw new Error('Lütfen önce Bot_token"ı config.js"de ayarlayın')
const { https_proxy } = process.env
const axins = axios.create(https_proxy ? { httpsAgent: new HttpsProxyAgent(https_proxy) } : {})

const FID_TO_NAME = {}

async function get_folder_name (fid) {
  let name = FID_TO_NAME[fid]
  if (name) return name
  name = await get_name_by_id(fid, !USE_PERSONAL_AUTH)
  return FID_TO_NAME[fid] = name
}

function send_help (chat_id) {
  const text = `
<b>Komut ｜ Açıklama</b>
➖➖➖➖➖➖➖➖➖➖➖➖
<pre>/reload</pre> <b>|</b> Görevi Yeniden Başlat
➖➖➖➖➖➖➖➖➖➖➖➖
<pre>/count FolderID [-u]</pre> <b>|</b> Boyutu Hesapla
- sonuna <pre>-u</pre> eklemek isteğe bağlıdır <i>(bilgi çevrimiçi olarak toplanacaktır)</i>
➖➖➖➖➖➖➖➖➖➖➖➖
<pre>/copy sourceID Hedef Kimliği [-u]</pre> <b>|</b> Dosyaları Klonla（Yeni bir Klasör oluşturacak）
- TargetID doldurulmazsa, varsayılan konuma kopyalanır (<pre>config.js</pre>'de ayarlanır)
- sonuna <pre>-u</pre> eklemek isteğe bağlıdır <i>(bilgi çevrimiçi olarak toplanacaktır)</i>
➖➖➖➖➖➖➖➖➖➖➖➖
<pre>/task</pre> <b>|</b> Çalışan görevle ilgili bilgileri gösterir
⁍ Örnek: 
<pre>/task</pre> <b>|</b> Çalışan Tüm Görevlerin Ayrıntılarını Döndürür.
<pre> /task [İD]<|pre> <b>/</b> Belirli Bir Görevin Bilgilerini Döndürür.
<pre>/task all</pre> <b>/</b> Tüm Görevlerin Listesini Döndürür.
<pre>/ görev temizle</pre> <b>|</b> Tamamlanan Tüm Görevleri Temizle.
<pre>/task rm [İD]<|pre> <b>/</b> Belirli Bir Görevi Silin.
➖➖➖➖➖➖➖➖➖➖➖➖
<pre>/bm [eylem] [takma ad] [hedef]</pre> <b>|</b> Yer İşareti olarak ortak bir Klasör Kimliği ekleyin
- <i>Aynı hedef klasöre birden çok kez klonlanırken yardımcı olur</i>
⁍ Örnek: 
<pre>/bm</pre> <b>/</b> Tüm yer imlerini gösterir
<pre>/bm film klasörü kimliğini ayarla<|pre> <b>/</b> Film adına göre yer imi ekle
<pre>/bm filmi kaldır</pre> <b>/</b> Bu yer imini sil
`
  return sm({ chat_id, text, parse_mode: 'HTML' })
}

function send_bm_help (chat_id) {
  const text = `<pre>/bm [action] [alias] [target]</pre> <b>|</b> Add a common FolderID as Bookmark
- <i>Helpful while cloning to same destination folder multiple times</i>
⁍ Example：
<pre>/bm</pre> <b>|</b> Shows all bookmarks
<pre>/bm set movie folder-id</pre> <b>|</b> Add a Bookmark by the name movie
<pre>/bm unset movie</pre> <b>|</b> Delete this bookmark
`
  return sm({ chat_id, text, parse_mode: 'HTML' })
}

function send_task_help (chat_id) {
  const text = `<pre>/task</pre> <b>|</b> Shows info about the running task
⁍ Example：
<pre>/task</pre> <b>|</b> Return Details Of All Running Tasks.
<pre>/task [ID]</pre> <b>|</b> Return Info Of Specific Task.
<pre>/task all</pre> <b>|</b> Return The List Of All Tasks.
<pre>/task clear</pre> <b>|</b> Clear All Completed Tasks.
<pre>/task rm [ID]</pre> <b>|</b> Delete Specific Task
`
  return sm({ chat_id, text, parse_mode: 'HTML' })
}

function clear_tasks (chat_id) {
  const finished_tasks = db.prepare('select id from task where status=?').all('finished')
  finished_tasks.forEach(task => rm_task({ task_id: task.id }))
  sm({ chat_id, text: 'Tamamlanan tüm görevler temizlendi' })
}

function rm_task ({ task_id, chat_id }) {
  const exist = db.prepare('select id from task where id=?').get(task_id)
  if (!exist) return sm({ chat_id, text: `<b>Task ID:</b> <pre>${task_id}</pre>. Does Not Exist`, parse_mode: 'HTML' })
  db.prepare('delete from task where id=?').run(task_id)
  db.prepare('delete from copied where taskid=?').run(task_id)
  if (chat_id) sm({ chat_id, text: `<b>Task ID:</b> <pre>${task_id}</pre>. Deleted`, parse_mode: 'HTML' })
}

function send_all_bookmarks (chat_id) {
  let records = db.prepare('select alias, target from bookmark').all()
  if (!records.length) return sm({ chat_id, text: 'No Bookmarks Found' })
  const tb = new Table({ style: { head: [], border: [] } })
  const headers = ['Name', 'FolderID']
  records = records.map(v => [v.alias, v.target])
  tb.push(headers, ...records)
  const text = tb.toString().replace(/─/g, '—')
  return sm({ chat_id, text: `<pre>${text}</pre>`, parse_mode: 'HTML' })
}

function set_bookmark ({ chat_id, alias, target }) {
  const record = db.prepare('select alias from bookmark where alias=?').get(alias)
  if (record) return sm({ chat_id, text: 'Aynı ada sahip başka bir Favori Klasör var' })
  db.prepare('INSERT INTO bookmark (alias, target) VALUES (?, ?)').run(alias, target)
  return sm({ chat_id, text: `<b>Bookmark Successfully Set</b>： <pre>${alias}</pre> <b>|</b> <pre>${target}</pre>`, parse_mode: 'HTML' })
}

function unset_bookmark ({ chat_id, alias }) {
  const record = db.prepare('select alias from bookmark where alias=?').get(alias)
  if (!record) return sm({ chat_id, text: 'Bu Adla Yer İşareti bulunamadı' })
  db.prepare('delete from bookmark where alias=?').run(alias)
  return sm({ chat_id, text: `<b>Bookmark Successfully Deleted</b>: <pre>${alias}</pre>`, parse_mode: 'HTML' })
}

function get_target_by_alias (alias) {
  const record = db.prepare('select target from bookmark where alias=?').get(alias)
  return record && record.target
}

function get_alias_by_target (target) {
  const record = db.prepare('select alias from bookmark where target=?').get(target)
  return record && record.alias
}

function send_choice ({ fid, chat_id }) {
  return sm({
    chat_id,
    text: `Drive ID: ${fid}, \nChoose what would you like to do`,
    reply_markup: {
      inline_keyboard: [
        [
          { text: 'Boyut', callback_data: `count ${fid}` },
          { text: 'Kopyala', callback_data: `copy ${fid}` }
        ],
        [
          { text: 'Yenile', callback_data: `update ${fid}` },
          { text: 'Temizle', callback_data: `clear_button` }
        ]
      ].concat(gen_bookmark_choices(fid))
    }
  })
}

// console.log(gen_bookmark_choices())
function gen_bookmark_choices (fid) {
  const gen_choice = v => ({ text: `Clone to ${v.alias}`, callback_data: `copy ${fid} ${v.alias}` })
  const records = db.prepare('select * from bookmark').all()
  const result = []
  for (let i = 0; i < records.length; i += 2) {
    const line = [gen_choice(records[i])]
    if (records[i + 1]) line.push(gen_choice(records[i + 1]))
    result.push(line)
  }
  return result
}

async function send_all_tasks (chat_id) {
  let records = db.prepare('görevden kimliği, durumu, time"ı seçin').all()
  if (!records.length) return sm({ chat_id, text: 'Veritabanında görev kaydı yok' })
  const tb = new Table({ style: { head: [], border: [] } })
  const headers = ['ID', 'status', 'ctime']
  records = records.map(v => {
    const { id, status, ctime } = v
    return [id, status, dayjs(ctime).format('YYYY-MM-DD HH:mm:ss')]
  })
  tb.push(headers, ...records)
  const text = tb.toString().replace(/─/g, '—')
  const url = `https://api.telegram.org/bot${tg_token}/sendMessage`
  return axins.post(url, {
    chat_id,
    parse_mode: 'HTML',
    text: `<b>All Clone Tasks</b>：\n<pre>${text}</pre>`
  }).catch(err => {
    console.error(err.message)
    // const description = err.response && err.response.data && err.response.data.description
    // if (description && description.includes('message is too long')) {
    const text = [headers].concat(records.slice(-100)).map(v => v.join('\t')).join('\n')
    return sm({ chat_id, parse_mode: 'HTML', text: `<b>Last 100 tasks</b>:\n${text}` })
  })
}

async function get_task_info (task_id) {
  const record = db.prepare('select * from task where id=?').get(task_id)
  if (!record) return {}
  const { source, target, status, mapping, ctime, ftime } = record
  const { copied_files } = db.prepare('select count(fileid) as copied_files from copied where taskid=?').get(task_id)
  const folder_mapping = mapping && mapping.trim().split('\n')
  const new_folder = folder_mapping && folder_mapping[0].split(' ')[1]
  const { summary } = db.prepare('select summary from gd where fid=?').get(source) || {}
  const { file_count, folder_count, total_size } = summary ? JSON.parse(summary) : {}
  const total_count = (file_count || 0) + (folder_count || 0)
  const copied_folders = folder_mapping ? (folder_mapping.length - 1) : 0
  let text = '<b>👤 Görev No</b>： <pre>' + task_id + '</pre>\n'
  const folder_name = await get_folder_name(source)
  text += '<b>📁 Kaynak</b>：' + gen_link(source, folder_name) + '\n'
  text += '<b>📁 Hedef</b>：' + gen_link(target, get_alias_by_target(target)) + '\n'
  text += '<b>📁 Drive_Link</b>：' + (new_folder ? gen_link(new_folder) : 'Not Created yet') + '\n'
  text += '<b>📝 Görev Durum</b>： <pre>' + status + '</pre>\n'
  text += '<b>🏁 Start</b>： <pre>' + dayjs(ctime).format('YYYY-MM-DD HH:mm:ss') + '</pre>\n'
  text += '<b>⛔️ End</b>： <pre>' + (ftime ? dayjs(ftime).format('YYYY-MM-DD HH:mm:ss') : 'Not Done') + '</pre>\n'
  text += '<b>🗁 Klasor Durum</b>： <pre>' + copied_folders + '/' + (folder_count === undefined ? 'Unknown' : folder_count) + '</pre>\n'
  text += '<b>🗄️ Dosya Durum</b>： <pre>' + copied_files + '/' + (file_count === undefined ? 'Unkno wn' : file_count) + '</pre>\n'
  text += '<b>🔄 Total Ilerleme</b>： <pre>' + ((copied_files + copied_folders) * 100 / total_count).toFixed(2) + '%</pre>\n'
  text += '<b>🚀 Toplam Boyut</b>： <pre>' + (total_size || 'Unknown') + '</pre>'
  return { text, status, folder_count }
}

async function send_task_info ({ task_id, chat_id }) {
  const { text, status, folder_count } = await get_task_info(task_id)
  if (!text) return sm({ chat_id, text: `<b>Task ID Does Not Exist In The Database：</b> <pre>${task_id}</pre>`, parse_mode: 'HTML' })
  const url = `https://api.telegram.org/bot${tg_token}/sendMessage`
  let message_id
  try {
    const { data } = await axins.post(url, { chat_id, text, parse_mode: 'HTML' })
    message_id = data && data.result && data.result.message_id
  } catch (e) {
    console.log('fail to send message to tg', e.message)
  }
  // get_task_info crash cpu when the number of Folders is too large，In the future, it is better to save the mapping as a separate table
  if (!message_id || status !== 'copying') return
  const loop = setInterval(async () => {
    const { text, status } = await get_task_info(task_id)
    // TODO check if text changed
    if (status !== 'copying') clearInterval(loop)
    sm({ chat_id, message_id, text, parse_mode: 'HTML' }, 'editMessageText')
  }, 10 * 1000)
}

async function tg_copy ({ fid, target, chat_id, update }) { // return task_id
  target = target || DEFAULT_TARGET
  if (!target) return sm({ chat_id, text: 'Lütfen hedef kimliğini girin veya varsayılan klon hedef kimliğini şurada ayarlayın config.js first(DEFAULT_TARGET)' })

  const file = await get_info_by_id(fid, !USE_PERSONAL_AUTH)
  if (!file) {
    const text = `Unable to get info，Please check if the link is valid and the SAs have appropriate permissions：https://drive.google.com/drive/folders/${fid}`
    return sm({ chat_id, text })
  }
  if (file && file.mimeType !== 'application/vnd.google-apps.folder') {
    return copy_file(fid, target, !USE_PERSONAL_AUTH).then(data => {
      sm({ chat_id, parse_mode: 'HTML', text: `<b>File Copied Succesfully</b>： ${gen_link(target)}` })
    }).catch(e => {
      sm({ chat_id, text: `<b>Failed To Clone The File</b>： <pre>${e.message}</pre>`, parse_mode: 'HTML' })
    })
  }

  let record = db.prepare('select id, status from task where source=? and target=?').get(fid, target)
  if (record) {
    if (record.status === 'copying') {
      return sm({ chat_id, text: 'SourceID ve DestinationID ile Görev Halihazırda Devam Ediyor，\nTip /task ' + record.id })
    } else if (record.status === 'Bitti') {
      sm({ chat_id, text: `<b>Existing Task Detected</b> <pre>${record.id}</pre> ,Started Cloning`, parse_mode: 'HTML' })
    }
  }

  real_copy({ source: fid, update, target, service_account: !USE_PERSONAL_AUTH, is_server: true })
    .then(async info => {
      if (!record) record = {} // Prevent infinite loop
      if (!info) return
      const { task_id } = info
      const { text } = await get_task_info(task_id)
      sm({ chat_id, text, parse_mode: 'HTML' })
    })
    .catch(err => {
      const task_id = record && record.id
      if (task_id) db.prepare('update task set status=? where id=?').run('error', task_id)
      if (!record) record = {}
      console.error('Kopyalama Başarısız', fid, '-->', target)
      console.error(err)
      sm({ chat_id, text: (task_id || '') + `<b>Task Error</b>：<pre>${err.message}</pre>`, parse_mode: 'HTML' })
    })

  while (!record) {
    record = db.prepare('select id from task where source=? and target=?').get(fid, target)
    await sleep(1000)
  }
  return record.id
}

function sleep (ms) {
  return new Promise((resolve, reject) => {
    setTimeout(resolve, ms)
  })
}

function reply_cb_query ({ id, data }) {
  const url = `https://api.telegram.org/bot${tg_token}/answerCallbackQuery`
  return axins.post(url, {
    callback_query_id: id,
    text: 'Görevi Başlat ' + data
  })
}

async function send_count ({ fid, chat_id, update }) {
  const gen_text = payload => {
    const { obj_count, processing_count, pending_count } = payload || {}
    const now = dayjs().format('YYYY-MM-DD HH:mm:ss')
    return `Size：${gen_link(fid)}
Time：${now}
Number of Files：${obj_count || ''}
${pending_count ? ('Bekliyor：' + pending_count) : ''}
${processing_count ? ('devam ediyor：' + processing_count) : ''}`
  }

  const url = `https://api.telegram.org/bot${tg_token}/sendMessage`
  let response
  try {
    response = await axins.post(url, { chat_id, text: `<b>Started</b>: <pre>${fid}</pre>.\nCollecting Files Stats,Please Wait.\nIt Is Recommended Not To Start Cloning Before The Stats Is Collected.`, parse_mode: 'HTML' })
  } catch (e) {}
  const { data } = response || {}
  const message_id = data && data.result && data.result.message_id
  const message_updater = payload => sm({
    chat_id,
    message_id,
    parse_mode: 'HTML',
    text: gen_text(payload)
  }, 'editMessageText')

  const service_account = !USE_PERSONAL_AUTH
  const table = await gen_count_body({ fid, update, service_account, type: 'tg', tg: message_id && message_updater })
  if (!table) return sm({ chat_id, parse_mode: 'HTML', text: gen_link(fid) + ' bilgi alınamadı' })
  const gd_link = `https://drive.google.com/drive/folders/${fid}`
  const name = await get_folder_name(fid)
  return axins.post(url, {
    chat_id,
    parse_mode: 'HTML',
    text: `<b>Source Folder Name</b>：${name}
<b>Source Folder Link</b>：${gd_link}
<pre>${table}</pre>`
  }).catch(async err => {
    console.log(err.message)
    // const description = err.response && err.response.data && err.response.data.description
    // const too_long_msgs = ['request entity too large', 'message is too long']
    // if (description && too_long_msgs.some(v => description.toLowerCase().includes(v))) {
    const limit = 20
    const table = await gen_count_body({ fid, type: 'tg', service_account: !USE_PERSONAL_AUTH, limit })
    return sm({
      chat_id,
      parse_mode: 'HTML',
      text: `<b>Name</b>：${name}
<b>Link</b>： <a href="${gd_link}">${fid}</a>
<i>The Table Is Too Long, Only Showing The First ${limit}</i>
<pre>${table}</pre>`
    })
  })
}

function sm (data, endpoint) {
  endpoint = endpoint || 'sendMessage'
  const url = `https://api.telegram.org/bot${tg_token}/${endpoint}`
  return axins.post(url, data).catch(err => {
    // console.error('fail to post', url, data)
    console.error('tg"ye mesaj gönderilemedi:', err.message)
    const err_data = err.response && err.response.data
    err_data && console.error(err_data)
  })
}

function extract_fid (text) {
  text = text.replace(/^\/count/, '').replace(/^\/copy/, '').replace(/\\n/g, '').replace(/\\/g, '').trim()
  const [source, target] = text.split(' ').map(v => v.trim())
  if (validate_fid(source)) return source
  try {
    if (!text.startsWith('http')) text = 'https://' + text
    const u = new URL(text)
    if (u.pathname.includes('/folders/')) {
      return u.pathname.split('/').map(v => v.trim()).filter(v => v).pop()
    } else if (u.pathname.includes('/file/')) {
      const file_reg = /file\/d\/([a-zA-Z0-9_-]+)/
      const file_match = u.pathname.match(file_reg)
      return file_match && file_match[1]
    }
    return u.searchParams.get('id')
  } catch (e) {
    return ''
  }
}

function extract_from_text (text) {
  // const reg = /https?:\/\/drive.google.com\/[^\s]+/g
  const reg = /https?:\/\/drive.google.com\/[a-zA-Z0-9_\\/?=&-]+/g
  const m = text.match(reg)
  return m && extract_fid(m[0])
}

module.exports = { send_count, send_help, sm, extract_fid, reply_cb_query, send_choice, send_task_info, send_all_tasks, tg_copy, extract_from_text, get_target_by_alias, send_bm_help, send_all_bookmarks, set_bookmark, unset_bookmark, clear_tasks, send_task_help, rm_task }
