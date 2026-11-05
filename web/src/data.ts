import {z} from 'zod'

const activeSchema = z.object({
  format_version: z.literal(1), snapshot: z.string().regex(/^[a-f0-9]{64}$/),
  manifest: z.string(), index_date: z.string(),
})
const planSchema = z.object({plan_name: z.string()})
const manifestSchema = z.object({
  format_version: z.literal(1), snapshot: z.string(),
  plans: z.array(planSchema),
  sources: z.array(z.object({
    id: z.string(), filename: z.string(), data_date: z.string(), plans: z.array(planSchema),
  })),
  code_count: z.number(), disclaimer: z.string(),
  catalogue_buckets: z.array(z.string()), provider_buckets: z.array(z.string()),
})
const indexSchema = z.object({pages: z.number().int().nonnegative(), count: z.number().int().nonnegative()})
const codeInfoSchema = indexSchema.extend({key: z.string().regex(/^[a-f0-9]{64}$/)})
const rateSchema = z.object({
  price_key: z.string(), code_type: z.string(), code: z.string(), code_version: z.string(),
  amount: z.string().regex(/^\d+(\.\d+)?$/), price_type: z.string(), expiration: z.string(),
  service_codes: z.array(z.string()), billing_class: z.string(), setting: z.string(),
  modifiers: z.array(z.string()), additional_information: z.string().nullable(),
  groups: z.array(z.string()), networks: z.array(z.string()).nullable(), arrangement: z.string(),
  group_networks: z.record(z.string(), z.array(z.string())),
})
const groupCodeSchema = z.object({public_group: z.string(), code_type: z.string(), code: z.string()})
export type Manifest = z.infer<typeof manifestSchema>
export type Rate = z.infer<typeof rateSchema>
export type CodeInfo = z.infer<typeof codeInfoSchema>
export type Dataset = {base: URL; manifest: Manifest; indexDate: string}

export function matchesProviderNetwork(row: Pick<Rate, 'groups' | 'group_networks'>, provider: string[], npi: string, network: string) {
  return row.groups.some(group => (!npi || provider.includes(group)) &&
    (!network || row.group_networks[group]?.includes(network)))
}

export async function hash(text: string): Promise<string> {
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text))
  return Array.from(new Uint8Array(bytes), x => x.toString(16).padStart(2, '0')).join('')
}

async function get<T>(base: URL, path: string, schema: z.ZodType<T>, signal?: AbortSignal): Promise<T | null> {
  const url = new URL(path, base)
  if (url.origin !== base.origin || !url.pathname.startsWith(base.pathname)) {
    throw new Error('Dataset attempted to reference an invalid object path.')
  }
  const response = await fetch(url, {signal})
  if (response.status === 404) return null
  if (!response.ok) throw new Error(`Dataset request failed (${response.status}). Please retry.`)
  const bytes = Number(response.headers.get('Content-Length'))
  if (bytes > 4 * 1024 * 1024) throw new Error('Dataset object exceeds the supported size.')
  // Limit streamed bytes too; Content-Length can be missing or reflect compressed data.
  const reader = response.body?.getReader()
  if (!reader) throw new Error('Dataset response has no readable body.')
  const chunks: Uint8Array[] = []
  let total = 0
  try {
    while (true) {
      const {done, value} = await reader.read()
      if (done) break
      total += value.byteLength
      if (total > 4 * 1024 * 1024) throw new Error('Dataset object exceeds the supported size.')
      chunks.push(value)
    }
  } finally {
    await reader.cancel()
  }
  const body = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {body.set(chunk, offset); offset += chunk.byteLength}
  return schema.parse(JSON.parse(new TextDecoder().decode(body)))
}

export async function loadDataset(base: string, signal?: AbortSignal): Promise<Dataset> {
  const url = new URL(base.endsWith('/') ? base : base + '/', location.href)
  if (url.protocol !== 'https:' && url.hostname !== 'localhost' && url.hostname !== '127.0.0.1') {
    throw new Error('Public datasets must use HTTPS.')
  }
  const active = await get(url, 'active.json', activeSchema, signal)
  if (!active) throw new Error('No validated dataset has been published at this address.')
  if (active.manifest !== `snapshots/${active.snapshot}/manifest.json`) {
    throw new Error('Dataset pointer does not match its immutable snapshot.')
  }
  const manifest = await get(url, active.manifest, manifestSchema, signal)
  if (!manifest || manifest.snapshot !== active.snapshot) throw new Error('Dataset snapshot is incomplete.')
  return {base: new URL(`snapshots/${active.snapshot}/`, url), manifest, indexDate: active.index_date}
}

export async function findCode(data: Dataset, type: string, code: string, signal?: AbortSignal) {
  const digest = await hash(type + ':' + code)
  if (!data.manifest.catalogue_buckets.includes(digest.slice(0, 2))) return null
  const bucket = await get(data.base, `catalogue/${digest.slice(0, 2)}.json`,
    z.record(z.string(), codeInfoSchema), signal)
  if (!bucket) throw new Error('A procedure index bucket is missing.')
  const info = bucket[type + ':' + code] ?? null
  if (info && info.key !== digest) throw new Error('Procedure index key does not match the requested code.')
  return info
}

export async function providerGroups(data: Dataset, npi: string, signal?: AbortSignal) {
  const digest = await hash(npi)
  if (!data.manifest.provider_buckets.includes(digest.slice(0, 3))) return []
  const bucket = await get(data.base, `providers/${digest.slice(0, 3)}.json`,
    z.record(z.string(), z.array(z.string())), signal)
  if (!bucket) throw new Error('A provider index bucket is missing.')
  return bucket[npi] ?? []
}

export async function ratePage(data: Dataset, info: CodeInfo, page: number, signal?: AbortSignal) {
  const rows = await get(data.base, `codes/${info.key}/${page}.json`, z.array(rateSchema), signal)
  if (!rows) throw new Error('A rate page is missing from this snapshot.')
  return rows
}

export async function groupCodes(data: Dataset, group: string, page: number, signal?: AbortSignal) {
  const digest = await hash(group)
  const info = await get(data.base, `groups/${digest}/index.json`, indexSchema, signal)
  if (!info) throw new Error('Provider code index is incomplete.')
  const rows = await get(data.base, `groups/${digest}/${page}.json`, z.array(groupCodeSchema), signal)
  if (!rows) throw new Error('Provider code page is missing.')
  return {info, rows}
}
