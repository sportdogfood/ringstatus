"""Local, on-demand context checkpoint. No scheduler, network or business writes."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def assess(latest, previous, now, progress, remaining):
    reasons = []
    last_input = latest.get('last_token_usage', {}).get('input_tokens', 0)
    if last_input >= 100000:
        reasons.append('large-context: latest input >= 100000 tokens')
    elapsed = None
    if previous:
        elapsed = (now - datetime.fromisoformat(previous['checked_at'])).total_seconds() / 60
        if elapsed >= 30 and progress == previous.get('progress'):
            reasons.append('no-recorded-progress-since-check')
        old = previous.get('remaining_percent')
        if remaining is not None and old is not None and old - remaining >= 10:
            reasons.append('account-usage-dropped-at-least-10-points')
    return {'handoff_review': bool(reasons), 'reasons': reasons,
            'minutes_since_check': elapsed, 'latest_input_tokens': last_input}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--session', required=True)
    p.add_argument('--state', required=True)
    p.add_argument('--progress', required=True)
    p.add_argument('--remaining', type=float)
    a = p.parse_args()
    latest = {}
    with open(a.session, encoding='utf-8') as stream:
        for line in stream:
            try:
                item = json.loads(line)
            except ValueError:
                continue
            payload = item.get('payload', {})
            if item.get('type') == 'event_msg' and payload.get('type') == 'token_count' and payload.get('info'):
                latest = payload['info']
    if not latest:
        raise SystemExit('No token telemetry available; do not interpret as zero usage.')
    state = Path(a.state)
    previous = json.loads(state.read_text(encoding='utf-8')) if state.exists() else None
    if previous and previous.get('session') != str(Path(a.session).resolve()):
        previous = None
    now = datetime.now(timezone.utc)
    result = assess(latest, previous, now, a.progress, a.remaining)
    saved = dict(result, checked_at=now.isoformat(), progress=a.progress,
                 remaining_percent=a.remaining, session=str(Path(a.session).resolve()),
                 total_token_usage=latest.get('total_token_usage'))
    state.parent.mkdir(parents=True, exist_ok=True)
    state.write_text(json.dumps(saved, indent=2), encoding='utf-8')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
