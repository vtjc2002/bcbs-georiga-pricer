import type {Rate} from './data'

export function displayPrice(row: Pick<Rate, 'amount' | 'price_type'>) {
  if (row.price_type === 'percentage') {
    return `${row.amount.replace(/(\.\d*?[1-9])0+$|\.0+$/, '$1')}% of billed charges`
  }
  const [whole, fraction = ''] = row.amount.split('.')
  const dollars = `$${whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}.${fraction.replace(/0+$/, '').padEnd(2, '0')}`
  if (row.price_type === 'per diem') return `${dollars} / day`
  if (['negotiated', 'derived', 'fee schedule'].includes(row.price_type)) return dollars
  return `${row.amount} (${row.price_type}; unit not interpreted)`
}
