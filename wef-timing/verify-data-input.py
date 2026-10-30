"""Read-only recheck of the immutable input pinned by a scheduled run."""
import importlib.util,json,pathlib,sys
root=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('timing_data_input',root/'data-input.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
try:
    request=json.load(sys.stdin)
    result=module.project(root.parent/'wef-data/runtime/handoff',request['pointer'],request['context'])
    json.dump(result,sys.stdout,ensure_ascii=False,separators=(',',':'))
except Exception as error:
    print(str(error),file=sys.stderr);sys.exit(1)
