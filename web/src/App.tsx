import {useEffect, useRef, useState} from 'react'
import {Button, Flash, FormControl, Heading, Label, Select, Spinner, TextInput} from '@primer/react'
import {findCode, groupCodes, loadDataset, matchesProviderNetwork, providerGroups, ratePage} from './data'
import type {CodeInfo, Dataset, Rate} from './data'
import {displayPrice} from './format'

type Search = {type: string; code: string; npi: string; plan: string; network: string}
function initialSearch(): Search {
  const params = new URLSearchParams(location.hash.slice(1))
  return {
    type: params.get('type') || 'CPT', code: params.get('code') || '',
    npi: params.get('npi') || '', plan: params.get('plan') || '', network: params.get('network') || '',
  }
}
function message(error: unknown) {
  return error instanceof Error ? error.message : 'The request failed. Please retry.'
}
export function App() {
  const configured = import.meta.env.VITE_DATA_BASE_URL || ''
  const [endpoint, setEndpoint] = useState(configured)
  const [data, setData] = useState<Dataset>()
  const [search, setSearch] = useState(initialSearch)
  const [appliedSearch, setAppliedSearch] = useState(initialSearch)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [rows, setRows] = useState<Rate[]>([])
  const [info, setInfo] = useState<CodeInfo>()
  const [page, setPage] = useState(0)
  const [searched, setSearched] = useState(false)
  const [groups, setGroups] = useState<string[]>([])
  const [providerOnly, setProviderOnly] = useState(false)
  const [group, setGroup] = useState('')
  const [codeChoices, setCodeChoices] = useState<{code_type: string; code: string}[]>([])
  const [groupPage, setGroupPage] = useState(0)
  const [groupPages, setGroupPages] = useState(0)
  const request = useRef<AbortController | null>(null)

  useEffect(() => () => request.current?.abort(), [])
  useEffect(() => {
    if (!configured) return
    const controller = new AbortController()
    setBusy(true)
    loadDataset(configured, controller.signal).then(setData).catch(error => {
      if (!controller.signal.aborted) setError(message(error))
    }).finally(() => {if (!controller.signal.aborted) setBusy(false)})
    return () => controller.abort()
  }, [configured])

  function begin() {
    request.current?.abort()
    const controller = new AbortController()
    request.current = controller
    setError(''); setBusy(true)
    return controller
  }
  async function connect() {
    const controller = begin()
    setData(undefined); setRows([]); setSearched(false); setProviderOnly(false)
    try {setData(await loadDataset(endpoint, controller.signal))}
    catch (error) {if (!controller.signal.aborted) setError(message(error))}
    finally {if (!controller.signal.aborted) setBusy(false)}
  }
  async function showGroup(data: Dataset, group: string, page: number, signal: AbortSignal) {
    const result = await groupCodes(data, group, page, signal)
    if (signal.aborted) return
    setCodeChoices(result.rows); setGroup(group); setGroupPage(page); setGroupPages(result.info.pages)
  }
  async function execute(next: Search, nextPage = 0) {
    if (!data) return
    const controller = begin()
    setSearch(next); setRows([]); setInfo(undefined); setCodeChoices([])
    setProviderOnly(false); setSearched(false)
    try {
      const trimmed = {...next, type: next.type.trim(), code: next.code.trim(), npi: next.npi.trim()}
      if (!trimmed.code && !trimmed.npi) throw new Error('Enter a procedure code or a provider NPI.')
      if (trimmed.npi && !/^[1-9]\d{9}$/.test(trimmed.npi)) throw new Error('Enter a 10-digit provider NPI.')
      if (trimmed.plan && !data.manifest.plans.some(plan => plan.plan_name === trimmed.plan)) {
        throw new Error('The selected plan is not included in this dataset.')
      }
      const sourceIds = new Set(data.manifest.sources.filter(source =>
        !trimmed.plan || source.plans.some(plan => plan.plan_name === trimmed.plan)).map(source => source.id))
      const provider = trimmed.npi ? (await providerGroups(data, trimmed.npi, controller.signal))
        .filter(group => sourceIds.has(group.split(':')[0])) : []
      if (controller.signal.aborted) return
      setAppliedSearch(trimmed)
      setGroups(provider)
      const params = new URLSearchParams(trimmed)
      history.replaceState(null, '', '#' + params.toString())
      setSearched(true); setPage(nextPage)
      if (trimmed.npi && !provider.length) return
      if (!trimmed.code) {
        setProviderOnly(true)
        await showGroup(data, provider[0], 0, controller.signal)
        return
      }
      const code = await findCode(data, trimmed.type, trimmed.code, controller.signal)
      if (!code || controller.signal.aborted) return
      const results = await ratePage(data, code, nextPage, controller.signal)
      if (controller.signal.aborted) return
      if (results.some(row => row.code_type !== trimmed.type || row.code !== trimmed.code)) {
        throw new Error('Rate page does not match the requested procedure code.')
      }
      setInfo(code)
      setRows(results.filter(row =>
        sourceIds.has(row.price_key.split(':')[0]) &&
        matchesProviderNetwork(row, provider, trimmed.npi, trimmed.network),
      ))
    } catch (error) {if (!controller.signal.aborted) setError(message(error))}
    finally {if (!controller.signal.aborted) setBusy(false)}
  }
  useEffect(() => {
    const followLink = () => {
      const linked = initialSearch()
      setSearch(linked)
      if (!data) return
      if (linked.code || linked.npi) void execute(linked)
      else {
        request.current?.abort()
        setBusy(false); setRows([]); setInfo(undefined); setSearched(false)
        setProviderOnly(false); setError('')
      }
    }
    followLink()
    window.addEventListener('hashchange', followLink)
    return () => window.removeEventListener('hashchange', followLink)
  }, [data])
  async function changeGroup(nextGroup: string, nextPage: number) {
    if (!data) return
    const controller = begin()
    try {await showGroup(data, nextGroup, nextPage, controller.signal)}
    catch (error) {if (!controller.signal.aborted) setError(message(error))}
    finally {if (!controller.signal.aborted) setBusy(false)}
  }
  return <>
    <header className="topbar"><span className="brand-mark" aria-hidden="true">$</span>
      <span>Georgia <strong>price checker</strong></span><Label>Public data</Label>
    </header>
    <main>
      <div className="title-row"><div><p className="eyebrow">TRANSPARENCY IN COVERAGE</p>
        <Heading as="h1">Know the published price.</Heading>
        <p className="subtitle">Explore negotiated rates in selected Georgia Blue Cross Blue Shield network files.</p>
      </div></div>
      <Flash className="notice">These are negotiated rates, <strong>not your out-of-pocket cost</strong>.
        Deductibles, copays, eligibility, and additional facility or professional charges are not included.</Flash>
      <section className="panel" aria-labelledby="data-heading">
        <div className="panel-heading"><Heading as="h2" id="data-heading">Data source</Heading>
          <Label variant={data ? 'success' : 'secondary'}>{data ? 'Snapshot loaded' : 'Not connected'}</Label></div>
        <div className="endpoint">
          <FormControl><FormControl.Label>Public lookup dataset URL</FormControl.Label>
            <TextInput block value={endpoint} onChange={event => setEndpoint(event.target.value)}
              placeholder="https://your-storage.blob.core.windows.net/lookup/" />
            <FormControl.Caption>No account or insurance information is needed.</FormControl.Caption>
          </FormControl>
          <Button onClick={connect} disabled={busy || !endpoint.trim()}>Load dataset</Button>
        </div>
        {data && <div className="freshness">
          <span>Index updated: <strong>{data.indexDate}</strong></span>
          <span>Rate data: <strong>{[...new Set(data.manifest.sources.map(source => source.data_date))].join(', ')}</strong></span>
          <span>{data.manifest.sources.length} selected files · {data.manifest.code_count.toLocaleString()} codes</span>
          {data.manifest.sources.some(source => source.data_date.slice(0, 7) < data.indexDate.slice(0, 7)) &&
            <Label variant="attention">Rate data predates index</Label>}
        </div>}
      </section>
      {error && <Flash variant="danger" role="alert">{error}</Flash>}
      <section className="panel" aria-labelledby="search-heading">
        <div className="panel-heading"><Heading as="h2" id="search-heading">Find a rate</Heading><span>No sign-in required</span></div>
        <form onSubmit={event => {event.preventDefault(); void execute(search)}}>
          <div className="search-grid">
            <FormControl><FormControl.Label>Plan</FormControl.Label>
              <Select block value={search.plan} onChange={event => setSearch({...search, plan: event.target.value})}>
                <Select.Option value="">All linked plans</Select.Option>
                {data?.manifest.plans.map(plan => <Select.Option key={plan.plan_name} value={plan.plan_name}>{plan.plan_name}</Select.Option>)}
              </Select>
            </FormControl>
            <FormControl><FormControl.Label>Code system</FormControl.Label>
              <TextInput block value={search.type} onChange={event => setSearch({...search, type: event.target.value})} />
              <FormControl.Caption>For example CPT, HCPCS, or MS-DRG.</FormControl.Caption>
            </FormControl>
            <FormControl><FormControl.Label>Procedure code</FormControl.Label>
              <TextInput block value={search.code} onChange={event => setSearch({...search, code: event.target.value})} placeholder="Keep any leading zeros" />
            </FormControl>
            <FormControl><FormControl.Label>Provider NPI (optional)</FormControl.Label>
              <TextInput block inputMode="numeric" value={search.npi} onChange={event => setSearch({...search, npi: event.target.value})} placeholder="10-digit public identifier" />
            </FormControl>
            <FormControl><FormControl.Label>Network name (optional)</FormControl.Label>
              <TextInput block value={search.network} onChange={event => setSearch({...search, network: event.target.value})} placeholder="Exact disclosed network name" />
            </FormControl>
          </div>
          <div className="search-actions"><Button type="submit" variant="primary" disabled={!data || busy}>Search rates</Button>
            <span>Search an NPI alone to browse its provider groups and procedure codes.</span></div>
        </form>
      </section>
      {busy && <div className="loading" role="status"><Spinner size="small" /> Loading a bounded data page...</div>}
      {providerOnly && <section className="panel" aria-label="Provider procedure codes">
        <div className="panel-heading"><Heading as="h2">Provider procedure codes</Heading></div>
        <FormControl><FormControl.Label>Disclosed provider group</FormControl.Label>
          <Select block value={group} onChange={event => void changeGroup(event.target.value, 0)}>
            {groups.map(value => <Select.Option key={value} value={value}>{value}</Select.Option>)}
          </Select>
          <FormControl.Caption>Browse each source-scoped group separately. Network filtering applies when you open a code's rates.</FormControl.Caption>
        </FormControl>
        <div className="code-choices">{codeChoices.map(code => <Button key={code.code_type + code.code} onClick={() =>
          void execute({...search, type: code.code_type, code: code.code})}>{code.code_type} {code.code}</Button>)}</div>
        <div className="pagination">
          <Button disabled={busy || groupPage === 0} onClick={() => void changeGroup(group, groupPage - 1)}>Previous codes</Button>
          <span>Page {groupPage + 1} of {groupPages}</span>
          <Button disabled={busy || groupPage + 1 >= groupPages} onClick={() => void changeGroup(group, groupPage + 1)}>Next codes</Button>
        </div>
      </section>}
      {searched && !providerOnly && !busy && <section className="panel" aria-label="Rate results">
        <div className="panel-heading"><Heading as="h2">Published rates</Heading>
          <Label>{rows.length} matching rows on this page</Label></div>
        {!rows.length ? <div className="empty"><strong>No matching rates on this page.</strong>
          <p>{info && info.pages > 1 ? 'Filters apply to each bounded source page. Continue to the next page to find more matches.' :
            'Check the exact code, code system, NPI, or network. A missing result does not establish that a service is not covered.'}</p></div> :
          <div className="table-scroll"><table><thead><tr>
            <th>Published price</th><th>Rate context</th><th>Network / provider scope</th><th>Expiration</th>
          </tr></thead><tbody>{rows.map(row => <tr key={row.price_key}>
            <td><strong className="price">{displayPrice(row)}</strong><small>{row.price_type}</small></td>
            <td>{row.billing_class} · {row.setting}<small>{row.arrangement} · POS {row.service_codes.join(', ') || 'not supplied'}
              {row.modifiers.length > 0 && ` · Modifiers ${row.modifiers.join(', ')}`}</small>
              {row.additional_information && <small>{row.additional_information}</small>}</td>
            <td>{row.networks?.join(', ') || 'Not specified'}<small>{row.groups.length} source-scoped groups · {row.code_type} {row.code} ({row.code_version})</small></td>
            <td>{row.expiration === '9999-12-31' ? 'Not specified' : row.expiration}
              {row.expiration < new Date().toISOString().slice(0, 10) && <Label variant="attention">Expired</Label>}</td>
          </tr>)}</tbody></table></div>}
        {info && <div className="pagination">
          <Button disabled={page === 0} onClick={() => void execute(appliedSearch, page - 1)}>Previous</Button>
          <span>Source page {page + 1} of {info.pages} · incompatible contexts must not be compared as equivalent</span>
          <Button disabled={page + 1 >= info.pages} onClick={() => void execute(appliedSearch, page + 1)}>Next</Button>
        </div>}
      </section>}
      <footer><strong>Understand the limits.</strong> Regional disclosures linked to two Microsoft/Premera plans,
        not every Georgia BCBS plan or every file for those plans. Prices do not confirm eligibility,
        service availability, or total episode cost. Source descriptions are not redistributed.
        This independent project is not endorsed by GitHub, Microsoft, Premera, or Anthem.
        {data && <details><summary>Source files and provenance</summary><ul>{data.manifest.sources.map(source =>
          <li key={source.id}>{source.filename} · data date {source.data_date}</li>)}</ul></details>}
      </footer>
    </main>
  </>
}
