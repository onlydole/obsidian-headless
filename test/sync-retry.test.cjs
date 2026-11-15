'use strict'

const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const { test } = require('node:test')
const vm = require('node:vm')

// The repository ships the bundled CLI. Load its actual engine without running
// command parsing, opening a vault, requiring native modules or authenticating.
const bundle = fs.readFileSync(path.join(__dirname, '..', 'cli.js'), 'utf8')
const start = bundle.indexOf('var is=class') + 'var is='.length
const end = bundle.indexOf(';function ss(', start)
assert.ok(start > 'var is='.length && end > start, 'sync engine bundle anchors changed')
const engineSource = '(' + bundle.slice(start, end) + ')'

function fixture(mode = 'bidirectional') {
  const clock = { now: 1_000_000 }
  const intervals = []
  const operations = []
  const calls = { scans: 0, waits: 0, clearedIntervals: 0 }
  class ClockDate extends Date {
    static now() { return clock.now }
  }
  const Engine = vm.runInNewContext(engineSource, {
    process: { platform: 'linux' }, Buffer, Date: ClockDate,
    console: { error() {}, debug() {} },
    setInterval(callback, delay) {
      intervals.push({ callback, delay })
      return intervals.length
    },
    clearInterval() { calls.clearedIntervals++ },
    Jd: async () => {
      // Fail promptly on the original tight loop rather than hanging the test.
      if (++calls.waits > 20) throw new Error('repeated scans with no runnable work')
    },
    Tc: () => true,
    Ne: value => value,
    $: value => value.split('.').at(-1),
    ne: async bytes => crypto.createHash('sha256').update(bytes).digest('hex'),
  })
  const engine = Object.create(Engine.prototype)
  const stateStore = {}
  for (const name of ['setLocalFile', 'deleteLocalFile', 'setServerFile',
    'deleteServerFile', 'deletePendingFile', 'close']) {
    stateStore[name] = (...args) => operations.push([name, ...args])
  }
  const server = {
    lastNetworkRequestTs: 0,
    perFileMax: 1_000_000,
    disconnect() { operations.push(['disconnect']) },
    async push(filePath, previousPath, folder, deleted, ctime, mtime, hash) {
      operations.push(['push', filePath])
      engine.serverFiles[filePath] = { ...engine.localFiles[filePath],
        path: filePath, folder, deleted, ctime, mtime, hash }
    },
  }
  Object.assign(engine, {
    localFiles: {}, serverFiles: {}, newServerFiles: [], fileRetry: {},
    skippedFiles: {}, scanSpecialFileQueue: [], scanSpecialFiles: false,
    stateStore, filter: { configDir: '.obsidian', allowSyncFile: () => true },
    adapter: { readBinary: async () => Buffer.from('# Test\n'),
      stopWatch() {}, thingsHappening: { cancel() {} } },
    syncMode: mode, ready: true, loaded: true, continuous: true,
    syncing: false, syncingPath: '', server,
    getServer: async () => {
      calls.scans++
      return server
    },
    log: (...args) => operations.push(['log', ...args]),
    syncFileDown: async (_server, file) => {
      operations.push(['download', file.path])
      engine.localFiles[file.path] = { ...file }
    },
  })
  return { engine, clock, intervals, operations, calls }
}

function file(filePath) {
  return { path: filePath, folder: false, deleted: false, hash: 'content-hash',
    size: 10, uid: 1, ctime: 1, mtime: 2, synctime: 0 }
}

for (const mode of ['bidirectional', 'pull-only', 'mirror-remote']) {
  test(`${mode} yields when the remaining remote work is in backoff`, async () => {
    const { engine, clock, operations, calls } = fixture(mode)
    engine.newServerFiles.push(file('waiting.md'))
    engine.fileRetry['waiting.md'] = { ts: clock.now + 300_000, count: 6 }

    await engine.requestSync()

    assert.equal(calls.scans, 1)
    assert.equal(calls.waits, 0)
    assert.equal(engine.syncing, false)
    assert.equal(engine.newServerFiles.length, 1)
    assert.equal(engine.fileRetry['waiting.md'].count, 6)
    assert.deepEqual(operations, [])
  })
}

for (const size of [10, 20_000, 200_000]) {
  test(`local upload cooldown yields for a ${size}-byte file`, async () => {
    const { engine, clock, calls, operations } = fixture()
    engine.localFiles['recent.md'] = { ...file('recent.md'), size, synctime: clock.now }

    await engine.requestSync()

    assert.equal(calls.scans, 1)
    assert.equal(calls.waits, 0)
    assert.deepEqual(operations, [])
    assert.equal(engine.localFiles['recent.md'].synctime, clock.now)
  })
}

test('a failed download enters backoff and the next request yields', async () => {
  const { engine, clock, calls } = fixture()
  engine.newServerFiles.push(file('waiting.md'))
  engine.syncFileDown = async () => {
    throw Object.assign(new Error('synthetic filesystem failure'), { code: 'ENAMETOOLONG' })
  }

  await engine.requestSync()
  assert.equal(engine.fileRetry['waiting.md'].ts, clock.now + 10_000)
  assert.equal(engine.fileRetry['waiting.md'].count, 1)

  await engine.requestSync()

  assert.equal(calls.scans, 2)
  assert.equal(calls.waits, 0)
  assert.equal(engine.newServerFiles.length, 1)
  assert.equal(engine.fileRetry['waiting.md'].count, 1)
})

test('another runnable remote file is processed while one path waits', async () => {
  const { engine, clock, operations, calls } = fixture()
  engine.newServerFiles.push(file('waiting.md'), { ...file('ready.md'), uid: 2 })
  engine.fileRetry['waiting.md'] = { ts: clock.now + 300_000, count: 6 }

  await engine.requestSync()

  assert.equal(calls.scans, 2)
  assert.equal(calls.waits, 1)
  assert.deepEqual(engine.newServerFiles.map(item => item.path), ['waiting.md'])
  assert.ok(operations.some(item => item[0] === 'download' && item[1] === 'ready.md'))
  assert.ok(!operations.some(item => item[0] === 'push'))
  assert.ok(!operations.some(item => item[0] === 'log' && item[1] === 'Fully synced'))
})

test('an expired retry is processed on the next request', async () => {
  const { engine, clock, calls, operations } = fixture()
  engine.newServerFiles.push(file('waiting.md'))
  engine.fileRetry['waiting.md'] = { ts: clock.now + 10_000, count: 1 }
  await engine.requestSync()
  clock.now += 30_000

  await engine.requestSync()

  assert.equal(calls.scans, 3)
  assert.equal(engine.newServerFiles.length, 0)
  assert.equal(Object.keys(engine.fileRetry).length, 0)
  assert.ok(operations.some(item => item[0] === 'download'))
  assert.ok(operations.some(item => item[0] === 'log' && item[1] === 'Fully synced'))
})

test('one-shot sync stays alive and its existing timer completes a deferred retry', async () => {
  const { engine, clock, intervals, calls, operations } = fixture()
  engine.continuous = false
  engine.newServerFiles.push(file('waiting.md'))
  engine.fileRetry['waiting.md'] = { ts: clock.now + 10_000, count: 1 }
  let completed = false
  let syncError
  const running = engine.sync().then(() => { completed = true }, error => { syncError = error })
  await new Promise(resolve => setImmediate(resolve))

  assert.equal(syncError, undefined)
  assert.equal(completed, false)
  assert.equal(calls.scans, 1)
  assert.equal(intervals[0].delay, 30_000)
  assert.ok(!operations.some(item => item[0] === 'close'))

  clock.now += 30_000
  await intervals[0].callback()
  await running

  assert.equal(completed, true)
  assert.equal(engine.newServerFiles.length, 0)
  assert.equal(calls.clearedIntervals, 1)
  assert.ok(operations.some(item => item[0] === 'close'))
})

test('continuous sync can process newly arrived work after yielding', async () => {
  const { engine, clock, operations } = fixture()
  engine.newServerFiles.push(file('waiting.md'))
  engine.fileRetry['waiting.md'] = { ts: clock.now + 300_000, count: 6 }
  await engine.requestSync()
  engine.newServerFiles.push({ ...file('arrived.md'), uid: 2 })

  await engine.requestSync()

  assert.equal(engine.newServerFiles.length, 1)
  assert.ok(operations.some(item => item[0] === 'download' && item[1] === 'arrived.md'))
  assert.equal(engine.fileRetry['waiting.md'].count, 6)
})

test('an idle continuous sync still reports completion and clears stale retry state', async () => {
  const { engine, calls, operations } = fixture()
  engine.fileRetry['old.md'] = { ts: 0, count: 1 }

  await engine.requestSync()

  assert.equal(calls.scans, 1)
  assert.equal(calls.waits, 0)
  assert.equal(Object.keys(engine.fileRetry).length, 0)
  assert.ok(operations.some(item => item[0] === 'log' && item[1] === 'Fully synced'))
  assert.ok(!operations.some(item => item[0] === 'close'))
})

test('an ordinary remote download still drains immediately', async () => {
  const { engine, calls, operations } = fixture()
  engine.newServerFiles.push(file('ready.md'))

  await engine.requestSync()

  assert.equal(calls.scans, 2)
  assert.equal(calls.waits, 1)
  assert.equal(engine.newServerFiles.length, 0)
  assert.ok(operations.some(item => item[0] === 'download' && item[1] === 'ready.md'))
  assert.ok(operations.some(item => item[0] === 'log' && item[1] === 'Fully synced'))
})
