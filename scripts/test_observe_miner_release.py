import json
import unittest

from observe_miner_release import journal_counts, update_summary


class ReleaseObservationTests(unittest.TestCase):
    def test_journal_windows_do_not_double_count_boundaries(self):
        text = '\n'.join(json.dumps({'__REALTIME_TIMESTAMP': str(at),
            'MESSAGE': 'route="stats" status=429 duration_ms=0'}) for at in [99, 100, 199, 200])
        self.assertEqual(journal_counts(text, 100, 200), {'stats:429': 2})
        self.assertEqual(journal_counts(text, 200, 300), {'stats:429': 1})

    def test_adoption_starts_full_observation_window_and_ignores_other_versions(self):
        sample = {'at': 'test', 'errors': {}, 'versions': [{'miner_version': '0.2.170', 'active_workers': 1}]}
        state = update_summary({}, sample, 100, '0.2.17', 1000, 100)
        self.assertIsNone(state['first_adoption_epoch'])
        sample['versions'] = [{'miner_version': 'seine/0.2.17', 'active_workers': 3}]
        state = update_summary(state, sample, 200, '0.2.17', 1000, 100)
        self.assertEqual(state['status'], 'observing_adoption')
        self.assertEqual(state['latest_target_workers'], 3)
        state = update_summary(state, sample, 299, '0.2.17', 1000, 100)
        self.assertEqual(state['status'], 'observing_adoption')
        state = update_summary(state, sample, 300, '0.2.17', 1000, 100)
        self.assertEqual(state['status'], 'complete')

    def test_missing_adoption_expires_and_sampling_errors_are_visible(self):
        sample = {'at': 'test', 'errors': {'versions': 'TimeoutExpired'}}
        state = update_summary({}, sample, 100, '0.2.17', 10, 100)
        state = update_summary(state, sample, 110, '0.2.17', 10, 100)
        self.assertEqual(state['status'], 'no_adoption_before_deadline')
        self.assertEqual(state['samples_with_errors'], 2)


if __name__ == '__main__':
    unittest.main()
