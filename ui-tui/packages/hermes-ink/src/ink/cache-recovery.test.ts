import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import sliceAnsi, { sliceCacheSize } from '../utils/sliceAnsi.js'

import { evictInkCaches } from './cache-eviction.js'
import { createNode } from './dom.js'
import { evictLineWidthCache, lineWidth, lineWidthCacheSize } from './line-width-cache.js'
import measureText from './measure-text.js'
import { addPendingClear, consumeAbsoluteRemovedFlag, nodeCache, pendingClears } from './node-cache.js'
import { stringWidth, widthCacheSize } from './stringWidth.js'
import wrapText, { wrapCacheSize } from './wrap-text.js'

function cacheSizes() {
  return { lineWidth: lineWidthCacheSize(), slice: sliceCacheSize(), width: widthCacheSize(), wrap: wrapCacheSize() }
}

beforeEach(() => {
  evictInkCaches('all')
  consumeAbsoluteRemovedFlag()
})

afterEach(() => {
  evictInkCaches('all')
  consumeAbsoluteRemovedFlag()
})

describe('restored line-width cache', () => {
  it.each(['plain text', '\u001b[31mred\u001b[0m', '漢字', 'e\u0301', '🐈', ''])(
    'preserves actual display width before and after eviction for %j',
    text => {
      const expected = stringWidth(text)
      expect(lineWidth(text)).toBe(expected)
      const populated = lineWidthCacheSize()
      expect(lineWidth(text)).toBe(expected)
      expect(lineWidthCacheSize()).toBe(populated)
      evictLineWidthCache()
      expect(lineWidthCacheSize()).toBe(0)
      expect(lineWidth(text)).toBe(expected)
    }
  )

  it('keeps multiline measurement stable across a full cache eviction', () => {
    const text = '漢字\n\u001b[32mgreen\u001b[0m\ne\u0301'
    const expected = { width: 5, height: 3 }
    expect(measureText(text, Infinity)).toEqual(expected)
    evictInkCaches('all')
    expect(measureText(text, Infinity)).toEqual(expected)
  })
})

describe('restored unified eviction', () => {
  it('halves all populated caches and fully clears them without changing output', () => {
    const samples = Array.from({ length: 12 }, (_, index) => `漢字 ${index} colored text`)
    const outputs = samples.map(text => ({
      line: lineWidth(text),
      slice: sliceAnsi(text, 0, 5),
      wrap: wrapText(text, 8, 'wrap')
    }))
    const before = cacheSizes()
    expect(Object.values(before).every(size => size > 0)).toBe(true)
    const half = evictInkCaches('half')
    for (const key of Object.keys(before) as (keyof typeof before)[]) {
      expect(half[key]).toBe(Math.floor(before[key] / 2))
    }
    expect(cacheSizes()).toEqual(half)
    const cleared = evictInkCaches('all')
    expect(Object.values(cleared).every(size => size === 0)).toBe(true)
    expect(cacheSizes()).toEqual(cleared)
    expect(samples.map(text => ({
      line: lineWidth(text), slice: sliceAnsi(text, 0, 5), wrap: wrapText(text, 8, 'wrap')
    }))).toEqual(outputs)
  })
})

describe('restored node layout and removal state', () => {
  it('keeps layouts and pending rectangles associated with their own node', () => {
    const first = createNode('ink-box')
    const second = createNode('ink-box')
    const firstRect = { x: 1, y: 2, width: 3, height: 4 }
    const secondRect = { x: 5, y: 6, width: 7, height: 8 }
    nodeCache.set(first, firstRect)
    nodeCache.set(second, secondRect)
    expect(nodeCache.get(first)).toEqual(firstRect)
    expect(nodeCache.get(second)).toEqual(secondRect)
    addPendingClear(first, firstRect, false)
    addPendingClear(first, secondRect, false)
    expect(pendingClears.get(first)).toEqual([firstRect, secondRect])
    expect(pendingClears.has(second)).toBe(false)
    expect(consumeAbsoluteRemovedFlag()).toBe(false)
  })

  it('consumes absolute removal once without losing pending rectangles', () => {
    const parent = createNode('ink-box')
    const rect = { x: 0, y: 0, width: 2, height: 2 }
    addPendingClear(parent, rect, true)
    addPendingClear(parent, rect, false)
    expect(consumeAbsoluteRemovedFlag()).toBe(true)
    expect(consumeAbsoluteRemovedFlag()).toBe(false)
    expect(pendingClears.get(parent)).toEqual([rect, rect])
    addPendingClear(parent, rect, true)
    expect(consumeAbsoluteRemovedFlag()).toBe(true)
  })
})
