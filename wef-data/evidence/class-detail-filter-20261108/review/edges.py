import importlib.util,pathlib,unittest
p=pathlib.Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('filter_fixture',p/'test_filter.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Edges(unittest.TestCase):
 def setUp(self):
  self.c=m.ClassFilter();self.c.setUp();self.addCleanup(self.c.doCleanups)
 def test_failed_selected_detail_counts_and_skipped_membership(self):
  c=self.c;c.seed_class(111);r=c.collect([c.item(111),c.item(112)],detail_fails=True)
  self.assertEqual(r['counts']['class-detail-requested'],1);self.assertEqual(r['counts']['class-detail-skipped'],1);self.assertNotIn('/classes/111',c.calls);self.assertIn('/classes/112',c.calls)
  for row in c.api.db[c.ct]:self.assertNotIn(row['fields'].get(c.cf['record-status']['id']),['Absent','Dropped'])
 def test_dropped_reappearance_does_not_create_permanent_suppression(self):
  c=self.c;c.seed_class(111,'In Progress',0,'Dropped');r=c.collect([c.item(111)]);self.assertEqual(r['counts']['class-detail-skipped'],1);self.assertNotIn('/classes/111',c.calls)
  c.calls.clear();r=c.collect([c.item(111)]);self.assertEqual(r['counts']['class-detail-requested'],1);self.assertIn('/classes/111',c.calls)
 def test_zero_classes_and_string_verification(self):
  c=self.c;r=c.collect([]);self.assertEqual(r['counts']['class-detail-requested'],0);self.assertEqual(r['counts']['class-detail-skipped'],0)
  c.seed_class(111,'Completed','1');c.calls.clear();r=c.collect([c.item(111)]);self.assertEqual(r['counts']['class-detail-requested'],1)
if __name__=='__main__':unittest.main(verbosity=2)
