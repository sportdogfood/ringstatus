"""Offline test export of copied immutable producer evidence. No acquisition/runtime writes."""
import importlib.util,json,pathlib,sys
spec=importlib.util.spec_from_file_location('entry_projection',pathlib.Path(__file__).with_name('data-input.py'))
entry=importlib.util.module_from_spec(spec);spec.loader.exec_module(entry)
folder=pathlib.Path(sys.argv[1]);label=sys.argv[2];working_day=sys.argv[3]
pointer=json.loads((folder/(label+'-pointer.json')).read_text())
a=pointer['authority'];context={'show-key':a[0],'customer-id':int(a[0].split('|')[0]),'show-id':int(a[0].split('|')[1]),'start-date':a[1],'end-date':a[2],'selected-date':a[3]}
data=entry.project(folder/'retained',pointer,context,working_day)
json.dump({'data':data,'pointer':pointer,'context':context},sys.stdout,separators=(',',':'))
