"""Validate canonical wef-shows input; never discover or select a show here."""
import datetime
import json
from zoneinfo import ZoneInfo

SHOWS_BASE = 'appCaSmL819KK7UFJ'


def validate_context(value, now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if value.get('authority-base') != SHOWS_BASE:
        raise ValueError('Canonical wef-shows authority required')
    rows = value.get('records')
    if not isinstance(rows, list):
        raise ValueError('Explicit canonical record inventory required')
    defaults = [r for r in rows if r.get('default') is True]
    if len(defaults) != 1:
        raise ValueError('Canonical default must resolve exactly once')
    row = defaults[0]
    customer, sid = row.get('customer-id'), row.get('show-id')
    if type(customer) is not int or customer != 15 or type(sid) is not int or sid <= 0:
        raise ValueError('Invalid canonical customer/show identity')
    if row.get('show-key') != f'{customer}|{sid}':
        raise ValueError('Canonical show key mismatch')
    source_checked = datetime.datetime.fromisoformat(row['last-checked'].replace('Z', '+00:00'))
    if source_checked.tzinfo is None or now.tzinfo is None or source_checked > now:
        raise ValueError('Canonical source time invalid or future')
    checked = datetime.datetime.fromisoformat((row.get('authority-checked-at') or row['last-checked']).replace('Z', '+00:00'))
    if checked.tzinfo is None or now.tzinfo is None or checked > now:
        raise ValueError('Canonical authority stale, future or unzoned')
    expires = checked + datetime.timedelta(minutes=30)
    local = checked.astimezone(ZoneInfo('America/New_York'))
    if not row.get('authority-checked-at') and (local.hour, local.minute) >= (19, 30):
        # Canonical authority only: retain a successful closing-window check
        # until the next daily window's first 30 minutes, preserving source time.
        next_day = local.date() + datetime.timedelta(days=1)
        next_check = datetime.datetime.combine(next_day, datetime.time(16, 30),
                                               tzinfo=ZoneInfo('America/New_York'))
        expires = max(expires, next_check)
    if now > expires:
        raise ValueError('Canonical authority stale, future or unzoned')
    start, end, selected = [datetime.date.fromisoformat(row[k][:10])
                            for k in ('start-date', 'end-date', 'selected-date')]
    if not start <= selected <= end:
        raise ValueError('Canonical selected date outside show range')
    return dict(row)


def load_context(path):
    with open(path, encoding='utf-8') as source:
        return validate_context(json.load(source))


def validate_source_show(payload, context):
    show = payload.get('show', {})
    if show.get('show_id') != context['show-id']:
        raise ValueError('Source disagrees with canonical show identity')
    for source, canonical in [('start_date', 'start-date'), ('end_date', 'end-date')]:
        if str(show.get(source, ''))[:10] != context[canonical][:10]:
            raise ValueError('Source disagrees with canonical show dates')
    return show
