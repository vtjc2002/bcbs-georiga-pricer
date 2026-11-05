import {afterEach, describe, expect, it, vi} from 'vitest'
import {displayPrice} from './format'
import {findCode, hash, loadDataset, matchesProviderNetwork, providerGroups} from './data'

afterEach(() => vi.unstubAllGlobals())
describe('dataset boundary', () => {
  it('uses the same SHA256 key as the ETL', async () => {
    expect(await hash('abc')).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')
  })
  it('reports an unpublished dataset rather than showing fabricated rates', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', {status: 404})))
    await expect(loadDataset('https://example.com/lookup/')).rejects.toThrow('No validated dataset')
  })
  it('rejects a pointer escaping its immutable snapshot', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
      format_version: 1, snapshot: 'a'.repeat(64), manifest: '../private.json', index_date: '2026-11-02',
    }))))
    await expect(loadDataset('https://example.com/lookup/')).rejects.toThrow('pointer')
  })
  it('never displays percentage or per-diem rates as per-service dollar amounts', () => {
    const row = {amount: '40.50000000', price_type: 'percentage'}
    expect(displayPrice(row)).toBe('40.5% of billed charges')
    expect(displayPrice({...row, amount: '100.00000000', price_type: 'per diem'})).toContain('/ day')
    expect(displayPrice({...row, amount: '40'})).toBe('40% of billed charges')
    expect(displayPrice({amount: '12345678901234567890.12345678', price_type: 'negotiated'}))
      .toBe('$12,345,678,901,234,567,890.12345678')
  })
  it('requires the selected network and provider to share the same group', () => {
    const row = {groups: ['a', 'b'], group_networks: {a: ['Network A'], b: ['Network B']}}
    expect(matchesProviderNetwork(row, ['a'], '1111111111', 'Network B')).toBe(false)
    expect(matchesProviderNetwork(row, ['a'], '1111111111', 'Network A')).toBe(true)
  })
  it('distinguishes missing expected buckets from a legitimate no-match', async () => {
    const snapshot = 'a'.repeat(64)
    const catalogue = (await hash('CPT:00123')).slice(0, 2)
    const provider = (await hash('1111111111')).slice(0, 3)
    vi.stubGlobal('fetch', vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        format_version: 1, snapshot, manifest: `snapshots/${snapshot}/manifest.json`, index_date: '2026-11-02',
      })))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        format_version: 1, snapshot, plans: [], sources: [], code_count: 1, disclaimer: 'Test',
        catalogue_buckets: [catalogue], provider_buckets: [provider],
      })))
      .mockImplementation(() => Promise.resolve(new Response('', {status: 404}))))
    const data = await loadDataset('https://example.com/lookup/')
    await expect(findCode(data, 'CPT', '00123')).rejects.toThrow('procedure index bucket is missing')
    await expect(providerGroups(data, '1111111111')).rejects.toThrow('provider index bucket is missing')
    data.manifest.catalogue_buckets = []
    await expect(findCode(data, 'CPT', '00123')).resolves.toBeNull()
  })
  it('enforces the actual streamed object limit without a Content-Length header', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(' '.repeat(4 * 1024 * 1024 + 1))))
    await expect(loadDataset('https://example.com/lookup/')).rejects.toThrow('exceeds the supported size')
  })
})
