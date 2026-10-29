"""Offline adapter checks against the Data owner's frozen contract fixture."""
import copy, json, pathlib, unittest
from retained_input import project

FIXTURE = pathlib.Path(r'C:\Users\gombc\Documents\Codex\2026-10-23\find-all-docs-discussing-wef-people-4\outputs\wef-data-day-count-fixture-20261029.json')

class RetainedInputTests(unittest.TestCase):
    def setUp(self):
        self.value = json.loads(FIXTURE.read_bytes())

    def replay(self, value=None):
        v = value or self.value
        return project(v['head'], v['runs'], v['collections'], v['context'])

    def test_exact_retained_input_repeat_no_tenant_and_original_observation(self):
        before = copy.deepcopy(self.value)
        result = self.replay()
        self.assertEqual(result, self.replay())
        self.assertEqual(before, self.value)
        self.assertTrue(result['classes'])
        self.assertFalse(result['whole-day-complete'])
        for item in result['classes']:
            self.assertTrue(item['observedAt'])
            self.assertTrue(item['evidence']['collections'])
            self.assertEqual(item['payload']['show_id'], self.value['context']['show-id'])

    def test_tampered_retained_part_is_rejected(self):
        row = next(iter(self.value['collections'].values()))
        row['retained-json'] += ' '
        with self.assertRaises(ValueError): self.replay()

    def test_changed_canonical_context_is_rejected(self):
        self.value['context']['selected-date'] = '2099-01-01'
        with self.assertRaisesRegex(ValueError, 'authority'): self.replay()

    def test_failed_revision_is_not_empty_success(self):
        self.value['runs'][self.value['head']]['status'] = 'Failed'
        with self.assertRaisesRegex(ValueError, 'completed'): self.replay()

if __name__ == '__main__': unittest.main()
