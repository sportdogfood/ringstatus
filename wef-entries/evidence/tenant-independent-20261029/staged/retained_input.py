"""Candidate inert Entries adapter; consumes Data journals, never fetches source."""
import pathlib, sys
ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'wef-data'))
sys.path.insert(0, str(ROOT / 'wef-schedules' / 'tests'))
from wef_data_handoff import revision_chain
from day_counts import retained_units, instant
from data_contract import packed, digest


def project(head, runs, collections, context):
    chain = revision_chain(head, runs, collections)
    fence = [context[k] for k in ('show-key', 'start-date', 'end-date', 'selected-date')]
    latest = chain[-1][0]
    if latest.get('authority') != fence or latest.get('authority-before') != fence:
        raise ValueError('Canonical authority changed')
    if context['show-key'] != f"{context['customer-id']}|{context['show-id']}":
        raise ValueError('Canonical show identity mismatch')
    snapshots = {}
    for state, _ in chain:
        if state['show-key'] != context['show-key']:
            raise ValueError('Wrong retained show')
        for unit, refs in retained_units(state, collections):
            if unit['scope'] != 'class':
                continue
            payloads = unit['source']['classes']
            receipts = unit['metric'].get('requests', [])
            if len(payloads) != len(receipts):
                raise ValueError('Missing class observation receipt')
            for payload, receipt in zip(payloads, receipts):
                if str(payload.get('show_id')) != str(context['show-id']):
                    raise ValueError('Class show mismatch')
                related = payload['class_related_data']
                date = str(related['date'])[:10]
                if not context['start-date'][:10] <= date <= context['end-date'][:10]:
                    raise ValueError('Class date outside canonical show')
                observed = receipt['completed']
                instant(observed)
                key = f"{context['show-key']}|{date}|{related['ring']}|{payload['class']['class_id']}"
                candidate = {'payload': payload, 'observedAt': observed,
                             'evidence': {'head': head, 'scope': key,
                                          'collections': refs,
                                          'payload-sha256': digest(packed(payload))}}
                previous = snapshots.get(key)
                if previous and instant(previous['observedAt']) == instant(observed):
                    if previous['payload'] != payload:
                        raise ValueError('Conflicting equal-time class observations')
                if not previous or instant(previous['observedAt']) < instant(observed):
                    snapshots[key] = candidate
    # A missing class input is unknown, not proof of an empty show or day.
    return {'head': head, 'classes': [snapshots[k] for k in sorted(snapshots)],
            'coverage': 'retained-class-snapshots', 'whole-day-complete': False}
