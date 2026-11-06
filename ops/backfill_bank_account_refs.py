"""
One-time backfill: repoint Bank Balances f8 and Bank Transactions f13 from the
Bank Account's Record ID# (what the sync wrote before fix/bank-account-key-links)
to its Quickbooks ID (the Bank Accounts table's Quickbase key).

Dry run by default. --write applies the changes in batches, logs every change to
an undo CSV first, and re-reads both tables afterwards to verify.

Usage: python ops/backfill_bank_account_refs.py path/to/.env [--write] [--undo-log FILE]
"""
import csv
import json
import sys
import urllib.request
from collections import Counter

ACCOUNT_KEY = 6   # Bank Accounts: Quickbooks ID (table key)
TABLES = {        # table env var -> (reference field, extra fields to read)
    'BALANCES_TABLE_ID': (8, [7]),
    'TRANSACTIONS_TABLE_ID': (13, []),
}


def load_env(path):
    env = {}
    for line in open(path):
        if '=' in line and not line.lstrip().startswith('#'):
            k, v = line.rstrip('\n').split('=', 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


class Quickbase:
    def __init__(self, env):
        self.headers = {
            'QB-Realm-Hostname': f"{env['QUICKBASE_REALM']}.quickbase.com",
            'Authorization': f"QB-USER-TOKEN {env['QUICKBASE_TOKEN']}",
            'Content-Type': 'application/json',
        }

    def post(self, path, body):
        req = urllib.request.Request('https://api.quickbase.com/v1/' + path,
                                     json.dumps(body).encode(), self.headers, method='POST')
        return json.load(urllib.request.urlopen(req))

    def query_all(self, table, select):
        rows, skip = [], 0
        while True:
            page = self.post('records/query', {'from': table, 'select': select,
                                               'options': {'skip': skip, 'top': 1000}})
            rows += page['data']
            total = page['metadata']['totalRecords']
            skip += page['metadata']['numRecords']
            if skip >= total or not page['data']:
                break
        if len(rows) != total:
            raise RuntimeError(f'{table}: read {len(rows)} of {total} rows')
        return rows


def val(row, fid):
    v = row.get(str(fid), {}).get('value')
    return None if v in (None, '') else int(v)


def plan(qb, env):
    accounts = qb.query_all(env['ACCOUNTS_TABLE_ID'], [3, ACCOUNT_KEY])
    rid_to_key = {val(a, 3): val(a, ACCOUNT_KEY) for a in accounts}
    keys = set(rid_to_key.values())
    ambiguous = keys & set(rid_to_key)
    print(f'Bank Accounts: {len(accounts)} rows; Record ID# {min(rid_to_key)}-{max(rid_to_key)}, '
          f'Quickbooks ID {min(keys)}-{max(keys)}')
    if ambiguous:
        raise SystemExit(f'ABORT: values are both a Record ID# and a Quickbooks ID: {sorted(ambiguous)}')

    changes, ok = [], True
    for env_name, (ref, extra) in TABLES.items():
        table = env[env_name]
        rows = qb.query_all(table, [3, ref] + extra)
        tally = Counter()
        for r in rows:
            v = val(r, ref)
            if v in keys:
                tally['already Quickbooks ID'] += 1
            elif v in rid_to_key:
                tally['legacy Record ID# -> remap'] += 1
                changes.append((table, ref, val(r, 3), v, rid_to_key[v],
                                r.get('7', {}).get('value') if extra else None))
            elif v is None:
                tally['blank (left alone)'] += 1
            else:
                tally['orphan (left alone)'] += 1
        print(f'{env_name} {table} f{ref}: {len(rows)} rows', dict(tally))

    # Bank Balances must stay one row per account per day after remapping.
    balances = env['BALANCES_TABLE_ID']
    rows = qb.query_all(balances, [3, 7, 8])
    remap = {c[2]: c[4] for c in changes if c[0] == balances}
    pairs = Counter((remap.get(val(r, 3), val(r, 8)), r['7']['value']) for r in rows if val(r, 8) is not None)
    dupes = {p: n for p, n in pairs.items() if n > 1}
    if dupes:
        ok = False
        print(f'WARNING: {len(dupes)} account/day pairs would have >1 balance row, e.g. {list(dupes)[:5]}')
    return changes, ok


def apply(qb, changes, undo_log):
    with open(undo_log, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['table', 'field', 'record_id', 'old_value', 'new_value'])
        for table, ref, rid, old, new, _ in changes:
            w.writerow([table, ref, rid, old, new])
    print(f'Undo log written: {undo_log}')
    by_table = {}
    for table, ref, rid, old, new, _ in changes:
        by_table.setdefault((table, ref), []).append({'3': {'value': rid}, str(ref): {'value': new}})
    for (table, ref), data in by_table.items():
        updated = 0
        for i in range(0, len(data), 1000):
            meta = qb.post('records', {'to': table, 'data': data[i:i + 1000], 'mergeFieldId': 3})['metadata']
            if meta.get('lineErrors') or meta.get('createdRecordIds'):
                raise RuntimeError(f'{table}: unexpected write result {meta}')
            updated += len(meta.get('updatedRecordIds', [])) + len(meta.get('unchangedRecordIds', []))
        print(f'{table} f{ref}: {updated} of {len(data)} rows written')


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    env = load_env(sys.argv[1])
    undo_log = sys.argv[sys.argv.index('--undo-log') + 1] if '--undo-log' in sys.argv else 'bank_ref_backfill_undo.csv'
    qb = Quickbase(env)
    changes, ok = plan(qb, env)
    print(f'{len(changes)} rows to remap')
    if '--write' not in sys.argv:
        print('Dry run: nothing written.')
        return
    if not ok:
        raise SystemExit('ABORT: resolve the duplicate account/day pairs first.')
    if changes:
        apply(qb, changes, undo_log)
    remaining, _ = plan(qb, env)
    if remaining:
        raise SystemExit(f'VERIFY FAILED: {len(remaining)} legacy references remain')
    print('Verified: no legacy Record ID# references remain.')


if __name__ == '__main__':
    main()
