Use this model:

repo stays at C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus
scheduled task keeps pointing to:
C:\Windows\System32\cmd.exe
/d /c "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\run_tagger_task.cmd"
What you n




cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"
node .\schedules_calculatorv2.js



cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"
node .\schedules_dailyv2.js

PS C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus> node .\schedules_dailyv2.js
{
  "ok": true,
  "dry_run": false,
  "scope": {
    "heartbeat_record_id": "recJAbNwAwsgDImkR",
    "heartbeat_rid": "recJAbNwAwsgDImkR",
    "hb_at": "2026-04-15T19:45:18.745Z",
    "app_show_idv2": 200000059,
    "app_sql_datev2": "2026-04-15",
    "app_dow_rawv2": "Wed",
    "shifted_to_next_dayv2": false,
    "scope_key": "200000059|2026-04-15|Wed|0",
    "scope_run_id": "200000059-2026-04-15-1776282318",
    "heartbeat_time": "03:45 PM",
    "heartbeat_show_date": null,
    "raw_sql_date": "2026-04-15",
    "mode": "DAY",
    "set_to_default_app_sql_date": false,
    "default_app_sql_date_is": "2026-04-15",
    "show_app_sql_start_date": "2026-04-15",
    "show_app_sql_end_date": "2026-04-19",
    "show_app_name": "2026 ESP Spring 3 (#5028) CSI 3*",
    "app_sql_date_source": "raw_day",
    "candidate_app_sql_date": "2026-04-15"
  },
  "chosen_source": "dated_schedule",
  "heartbeat_patch_fields": {
    "shifted_to_next_day": false,
    "set_to_default_app_sql_date": false,
    "show_app_name": "2026 ESP Spring 3 (#5028) CSI 3*"
  },
  "row_count": 11,
  "creates_planned": 0,
  "updates_planned": 11,
  "drops_planned": 25,
  "existing_show_rows": 36,
  "heartbeat_view_rows": 36,
  "show_record_bound": true,
  "fetches": {
    "dated_schedule": {
      "url": "https://broad-tooth-b8ed.gombcg.workers.dev/schedule?date=2026-04-15&show_id=200000059&customer_id=15",
      "rows": 11,
      "schedule_show_datev2": "2026-04-15"
    },
    "empty_ping_schedule": {
      "url": "https://broad-tooth-b8ed.gombcg.workers.dev/schedule?date=00/00/00&show_id=200000059&customer_id=15",
      "rows": 11,
      "schedule_show_datev2": "2026-04-15"
    }
  },
  "writes": {
    "created": 0,
    "updated": 11,
    "dropped": 25,
    "create_failures": [],
    "update_failures": [],
    "drop_failures": []
  },
  "active_groups": {
    "table": "active_groups",
    "created_planned": 0,
    "updated_planned": 5,
    "inactivated_planned": 0,
    "writes": {
      "created": 0,
      "updated": 5,
      "inactivated": 0,
      "create_failures": [],
      "update_failures": [],
      "inactivate_failures": []
    },
    "skipped": false
  }
}


node .\schedules_calculatorv2.js

PS C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus> node .\schedules_calculatorv2.js
{"ok":false,"error":"Error: Airtable list failed (422) groups_live: {\"error\":{\"type\":\"UNKNOWN_FIELD_NAME\",\"message\":\"Unknown field name: \\\"stop_updating\\\"\"}}\n    at airtableList (C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\schedules_calculatorv2.js:256:13)\n    at process.processTicksAndRejections (node:internal/process/task_queues:104:5)\n    at async fetchGroupsLiveRows (C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\schedules_calculatorv2.js:434:16)\n    at async main (C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\schedules_calculatorv2.js:822:22)","calc_mode":"shadow","calc_version":"schedules_calculator_v2_1"}
PS C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus>

Yes. Those are the same OneDrive path on two machines, and telling you to “copy from that path to that path” was not useful.

The correct fix is:

1. Stop running the task from OneDrive on `LAPTOP-4JTHU84J`.
2. Make a separate local runtime folder there, for example:
```powershell
New-Item -ItemType Directory -Force -Path C:\ringstatus-task | Out-Null
robocopy "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus" "C:\ringstatus-task" /MIR
```

3. Point the scheduled task to that local copy:
```text
C:\Windows\System32\cmd.exe
/d /c "C:\ringstatus-task\run_tagger_task.cmd"
```

4. Run the task once manually.

Why this is the fix:
- same OneDrive path on both machines is still OneDrive-backed
- your error is Windows/Node failing to open files from that synced location on the laptop
- the runner files are now portable, so they can run from `C:\ringstatus-task`

From now on, when you need to refresh the task copy on the laptop, run only:
```powershell
robocopy "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus" "C:\ringstatus-task" /MIR
```

That is the path forward.


$root = "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"
$files = @(
  "schedule_normalizer_v2.js",
  "schedules_dailyv2.js",
  "schedules_calculatorv2.js",
  "trips_normalizer_v2.js",
  "trips_dailyv2.js",
  "trips_calculatorv2.js"
)

foreach ($f in $files) {
  $p = Join-Path $root $f
  attrib +P -U $p
}

foreach ($f in $files) {
  $p = Join-Path $root $f
  Get-Content $p -Head 1 | Out-Null
  Write-Host "OK $f"
}



[Wed 04/15/2026 14:52:30.41] SCHEDULES_CALCULATORV2 RUN 
node:fs:440
    return binding.readFileUtf8(path, stringToFlags(options.flag));
                   ^

Error: UNKNOWN: unknown error, open 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\schedules_calculatorv2.js'
    at Object.readFileSync (node:fs:440:20)
    at defaultLoadImpl (node:internal/modules/cjs/loader:1148:17)
    at loadSource (node:internal/modules/cjs/loader:1838:20)
    at Object..js (node:internal/modules/cjs/loader:1936:44)
    at Module.load (node:internal/modules/cjs/loader:1533:32)
    at Module._load (node:internal/modules/cjs/loader:1335:12)
    at wrapModuleLoad (node:internal/modules/cjs/loader:255:19)
    at Module.executeUserEntryPoint [as runMain] (node:internal/modules/run_main:154:5)
    at node:internal/main/run_main_module:33:47 {
  errno: -4094,
  code: 'UNKNOWN',
  syscall: 'open',
  path: 'C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\schedules_calculatorv2.js'
}

Node.js v24.14.1


Node.js v24.14.1
[Wed 04/15/2026 14:52:40.42] SCHEDULES_DAILYV2 RUN 
node:fs:440
    return binding.readFileUtf8(path, stringToFlags(options.flag));
                   ^

Error: UNKNOWN: unknown error, open 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\schedules_dailyv2.js'
    at Object.readFileSync (node:fs:440:20)
    at defaultLoadImpl (node:internal/modules/cjs/loader:1148:17)
    at loadSource (node:internal/modules/cjs/loader:1838:20)
    at Object..js (node:internal/modules/cjs/loader:1936:44)
    at Module.load (node:internal/modules/cjs/loader:1533:32)
    at Module._load (node:internal/modules/cjs/loader:1335:12)
    at wrapModuleLoad (node:internal/modules/cjs/loader:255:19)
    at Module.executeUserEntryPoint [as runMain] (node:internal/modules/run_main:154:5)
    at node:internal/main/run_main_module:33:47 {
  errno: -4094,
  code: 'UNKNOWN',
  syscall: 'open',
  path: 'C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\schedules_dailyv2.js'
}

Node.js v24.14.1
[Wed 04/15/2026 14:55:29.10] SCHEDULES_DAILYV2 RUN 
node:fs:440
    return binding.readFileUtf8(path, stringToFlags(options.flag));
                   ^

Error: UNKNOWN: unknown error, open 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\schedules_dailyv2.js'
    at Object.readFileSync (node:fs:440:20)
    at defaultLoadImpl (node:internal/modules/cjs/loader:1148:17)
    at loadSource (node:internal/modules/cjs/loader:1838:20)
    at Object..js (node:internal/modules/cjs/loader:1936:44)
    at Module.load (node:internal/modules/cjs/loader:1533:32)
    at Module._load (node:internal/modules/cjs/loader:1335:12)
    at wrapModuleLoad (node:internal/modules/cjs/loader:255:19)
    at Module.executeUserEntryPoint [as runMain] (node:internal/modules/run_main:154:5)
    at node:internal/main/run_main_module:33:47 {
  errno: -4094,
  code: 'UNKNOWN',
  syscall: 'open',
  path: 'C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\schedules_dailyv2.js'
}

Node.js v24.14.1


Information	4/15/2026 2:52:30 PM	102	Task completed	(2)	df11fc63-7b20-4964-8bbe-aa6b8bb231c3
Information	4/15/2026 2:52:30 PM	201	Action completed	(2)	df11fc63-7b20-4964-8bbe-aa6b8bb231c3
Information	4/15/2026 2:52:30 PM	102	Task completed	(2)	5c96b1dd-1d83-494d-9279-6daa391c3073
Information	4/15/2026 2:52:30 PM	201	Action completed	(2)	5c96b1dd-1d83-494d-9279-6daa391c3073
Information	4/15/2026 2:52:30 PM	110	Task triggered by user	Info	5c96b1dd-1d83-494d-9279-6daa391c3073
Information	4/15/2026 2:52:30 PM	200	Action started	(1)	5c96b1dd-1d83-494d-9279-6daa391c3073
Information	4/15/2026 2:52:30 PM	100	Task Started	(1)	5c96b1dd-1d83-494d-9279-6daa391c3073
Information	4/15/2026 2:52:30 PM	129	Created Task Process	Info	
Information	4/15/2026 2:52:29 PM	110	Task triggered by user	Info	df11fc63-7b20-4964-8bbe-aa6b8bb231c3
Information	4/15/2026 2:52:29 PM	200	Action started	(1)	df11fc63-7b20-4964-8bbe-aa6b8bb231c3
Information	4/15/2026 2:52:29 PM	100	Task Started	(1)	df11fc63-7b20-4964-8bbe-aa6b8bb231c3
Information	4/15/2026 2:52:29 PM	129	Created Task Process	Info	


Log Name:      Microsoft-Windows-TaskScheduler/Operational
Source:        Microsoft-Windows-TaskScheduler
Date:          4/15/2026 2:52:30 PM
Event ID:      110
Task Category: Task triggered by user
Level:         Information
Keywords:      
User:          SYSTEM
Computer:      LAPTOP-4JTHU84J
Description:
Task Scheduler launched "{5c96b1dd-1d83-494d-9279-6daa391c3073}"  instance of task "\epoch-tagger-local"  for user "gombc" .
Event Xml:
<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
  <System>
    <Provider Name="Microsoft-Windows-TaskScheduler" Guid="{de7b24ea-73c8-4a09-985d-5bdadcfa9017}" />
    <EventID>110</EventID>
    <Version>0</Version>
    <Level>4</Level>
    <Task>110</Task>
    <Opcode>0</Opcode>
    <Keywords>0x8000000000000000</Keywords>
    <TimeCreated SystemTime="2026-04-15T18:52:30.1854167Z" />
    <EventRecordID>9962</EventRecordID>
    <Correlation ActivityID="{5c96b1dd-1d83-494d-9279-6daa391c3073}" />
    <Execution ProcessID="3272" ThreadID="19800" />
    <Channel>Microsoft-Windows-TaskScheduler/Operational</Channel>
    <Computer>LAPTOP-4JTHU84J</Computer>
    <Security UserID="S-1-5-18" />
  </System>
  <EventData Name="TaskRunEvent">
    <Data Name="TaskName">\epoch-tagger-local</Data>
    <Data Name="InstanceId">{5c96b1dd-1d83-494d-9279-6daa391c3073}</Data>
    <Data Name="UserContext">gombc</Data>
  </EventData>
</Event>

Log Name:      Microsoft-Windows-TaskScheduler/Operational
Source:        Microsoft-Windows-TaskScheduler
Date:          4/15/2026 2:52:30 PM
Event ID:      129
Task Category: Created Task Process
Level:         Information
Keywords:      
User:          SYSTEM
Computer:      LAPTOP-4JTHU84J
Description:
Task Scheduler launch task "\epoch-tagger-local" , instance "C:\Windows\System32\cmd.exe"  with process ID 11652.
Event Xml:
<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
  <System>
    <Provider Name="Microsoft-Windows-TaskScheduler" Guid="{de7b24ea-73c8-4a09-985d-5bdadcfa9017}" />
    <EventID>129</EventID>
    <Version>0</Version>
    <Level>4</Level>
    <Task>129</Task>
    <Opcode>0</Opcode>
    <Keywords>0x8000000000000000</Keywords>
    <TimeCreated SystemTime="2026-04-15T18:52:30.1840740Z" />
    <EventRecordID>9959</EventRecordID>
    <Correlation />
    <Execution ProcessID="3272" ThreadID="19800" />
    <Channel>Microsoft-Windows-TaskScheduler/Operational</Channel>
    <Computer>LAPTOP-4JTHU84J</Computer>
    <Security UserID="S-1-5-18" />
  </System>
  <EventData Name="CreatedTaskProcess">
    <Data Name="TaskName">\epoch-tagger-local</Data>
    <Data Name="Path">C:\Windows\System32\cmd.exe</Data>
    <Data Name="ProcessID">11652</Data>
    <Data Name="Priority">16384</Data>
  </EventData>
</Event>


Log Name:      Microsoft-Windows-TaskScheduler/Operational
Source:        Microsoft-Windows-TaskScheduler
Date:          4/15/2026 2:52:30 PM
Event ID:      100
Task Category: Task Started
Level:         Information
Keywords:      (1)
User:          SYSTEM
Computer:      LAPTOP-4JTHU84J
Description:
Task Scheduler started "{5c96b1dd-1d83-494d-9279-6daa391c3073}" instance of the "\epoch-tagger-local" task for user "LAPTOP-4JTHU84J\gombc".
Event Xml:
<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
  <System>
    <Provider Name="Microsoft-Windows-TaskScheduler" Guid="{de7b24ea-73c8-4a09-985d-5bdadcfa9017}" />
    <EventID>100</EventID>
    <Version>0</Version>
    <Level>4</Level>
    <Task>100</Task>
    <Opcode>1</Opcode>
    <Keywords>0x8000000000000001</Keywords>
    <TimeCreated SystemTime="2026-04-15T18:52:30.1847647Z" />
    <EventRecordID>9960</EventRecordID>
    <Correlation ActivityID="{5c96b1dd-1d83-494d-9279-6daa391c3073}" />
    <Execution ProcessID="3272" ThreadID="19800" />
    <Channel>Microsoft-Windows-TaskScheduler/Operational</Channel>
    <Computer>LAPTOP-4JTHU84J</Computer>
    <Security UserID="S-1-5-18" />
  </System>
  <EventData Name="TaskStartEvent">
    <Data Name="TaskName">\epoch-tagger-local</Data>
    <Data Name="UserContext">LAPTOP-4JTHU84J\gombc</Data>
    <Data Name="InstanceId">{5c96b1dd-1d83-494d-9279-6daa391c3073}</Data>
  </EventData>
</Event>

Log Name:      Microsoft-Windows-TaskScheduler/Operational
Source:        Microsoft-Windows-TaskScheduler
Date:          4/15/2026 2:52:30 PM
Event ID:      200
Task Category: Action started
Level:         Information
Keywords:      
User:          SYSTEM
Computer:      LAPTOP-4JTHU84J
Description:
Task Scheduler launched action "C:\Windows\System32\cmd.exe" in instance "{5c96b1dd-1d83-494d-9279-6daa391c3073}" of task "\epoch-tagger-local".
Event Xml:
<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
  <System>
    <Provider Name="Microsoft-Windows-TaskScheduler" Guid="{de7b24ea-73c8-4a09-985d-5bdadcfa9017}" />
    <EventID>200</EventID>
    <Version>1</Version>
    <Level>4</Level>
    <Task>200</Task>
    <Opcode>1</Opcode>
    <Keywords>0x8000000000000000</Keywords>
    <TimeCreated SystemTime="2026-04-15T18:52:30.1847725Z" />
    <EventRecordID>9961</EventRecordID>
    <Correlation ActivityID="{5c96b1dd-1d83-494d-9279-6daa391c3073}" />
    <Execution ProcessID="3272" ThreadID="19800" />
    <Channel>Microsoft-Windows-TaskScheduler/Operational</Channel>
    <Computer>LAPTOP-4JTHU84J</Computer>
    <Security UserID="S-1-5-18" />
  </System>
  <EventData Name="ActionStart">
    <Data Name="TaskName">\epoch-tagger-local</Data>
    <Data Name="ActionName">C:\Windows\System32\cmd.exe</Data>
    <Data Name="TaskInstanceId">{5c96b1dd-1d83-494d-9279-6daa391c3073}</Data>
    <Data Name="EnginePID">11652</Data>
  </EventData>
</Event>

Log Name:      Microsoft-Windows-TaskScheduler/Operational
Source:        Microsoft-Windows-TaskScheduler
Date:          4/15/2026 2:52:30 PM
Event ID:      110
Task Category: Task triggered by user
Level:         Information
Keywords:      
User:          SYSTEM
Computer:      LAPTOP-4JTHU84J
Description:
Task Scheduler launched "{5c96b1dd-1d83-494d-9279-6daa391c3073}"  instance of task "\epoch-tagger-local"  for user "gombc" .
Event Xml:
<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
  <System>
    <Provider Name="Microsoft-Windows-TaskScheduler" Guid="{de7b24ea-73c8-4a09-985d-5bdadcfa9017}" />
    <EventID>110</EventID>
    <Version>0</Version>
    <Level>4</Level>
    <Task>110</Task>
    <Opcode>0</Opcode>
    <Keywords>0x8000000000000000</Keywords>
    <TimeCreated SystemTime="2026-04-15T18:52:30.1854167Z" />
    <EventRecordID>9962</EventRecordID>
    <Correlation ActivityID="{5c96b1dd-1d83-494d-9279-6daa391c3073}" />
    <Execution ProcessID="3272" ThreadID="19800" />
    <Channel>Microsoft-Windows-TaskScheduler/Operational</Channel>
    <Computer>LAPTOP-4JTHU84J</Computer>
    <Security UserID="S-1-5-18" />
  </System>
  <EventData Name="TaskRunEvent">
    <Data Name="TaskName">\epoch-tagger-local</Data>
    <Data Name="InstanceId">{5c96b1dd-1d83-494d-9279-6daa391c3073}</Data>
    <Data Name="UserContext">gombc</Data>
  </EventData>
</Event>


Log Name:      Microsoft-Windows-TaskScheduler/Operational
Source:        Microsoft-Windows-TaskScheduler
Date:          4/15/2026 2:52:30 PM
Event ID:      201
Task Category: Action completed
Level:         Information
Keywords:      
User:          SYSTEM
Computer:      LAPTOP-4JTHU84J
Description:
Task Scheduler successfully completed task "\epoch-tagger-local" , instance "{5c96b1dd-1d83-494d-9279-6daa391c3073}" , action "C:\Windows\System32\cmd.exe" with return code 2147942401.
Event Xml:
<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
  <System>
    <Provider Name="Microsoft-Windows-TaskScheduler" Guid="{de7b24ea-73c8-4a09-985d-5bdadcfa9017}" />
    <EventID>201</EventID>
    <Version>2</Version>
    <Level>4</Level>
    <Task>201</Task>
    <Opcode>2</Opcode>
    <Keywords>0x8000000000000000</Keywords>
    <TimeCreated SystemTime="2026-04-15T18:52:30.6621179Z" />
    <EventRecordID>9963</EventRecordID>
    <Correlation ActivityID="{5c96b1dd-1d83-494d-9279-6daa391c3073}" />
    <Execution ProcessID="3272" ThreadID="19800" />
    <Channel>Microsoft-Windows-TaskScheduler/Operational</Channel>
    <Computer>LAPTOP-4JTHU84J</Computer>
    <Security UserID="S-1-5-18" />
  </System>
  <EventData Name="ActionSuccess">
    <Data Name="TaskName">\epoch-tagger-local</Data>
    <Data Name="TaskInstanceId">{5c96b1dd-1d83-494d-9279-6daa391c3073}</Data>
    <Data Name="ActionName">C:\Windows\System32\cmd.exe</Data>
    <Data Name="ResultCode">2147942401</Data>
    <Data Name="EnginePID">11652</Data>
  </EventData>
</Event>

Log Name:      Microsoft-Windows-TaskScheduler/Operational
Source:        Microsoft-Windows-TaskScheduler
Date:          4/15/2026 2:52:30 PM
Event ID:      102
Task Category: Task completed
Level:         Information
Keywords:      (1)
User:          SYSTEM
Computer:      LAPTOP-4JTHU84J
Description:
Task Scheduler successfully finished "{5c96b1dd-1d83-494d-9279-6daa391c3073}" instance of the "\epoch-tagger-local" task for user "LAPTOP-4JTHU84J\gombc".
Event Xml:
<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
  <System>
    <Provider Name="Microsoft-Windows-TaskScheduler" Guid="{de7b24ea-73c8-4a09-985d-5bdadcfa9017}" />
    <EventID>102</EventID>
    <Version>0</Version>
    <Level>4</Level>
    <Task>102</Task>
    <Opcode>2</Opcode>
    <Keywords>0x8000000000000001</Keywords>
    <TimeCreated SystemTime="2026-04-15T18:52:30.6627472Z" />
    <EventRecordID>9964</EventRecordID>
    <Correlation ActivityID="{5c96b1dd-1d83-494d-9279-6daa391c3073}" />
    <Execution ProcessID="3272" ThreadID="19800" />
    <Channel>Microsoft-Windows-TaskScheduler/Operational</Channel>
    <Computer>LAPTOP-4JTHU84J</Computer>
    <Security UserID="S-1-5-18" />
  </System>
  <EventData Name="TaskSuccessEvent">
    <Data Name="TaskName">\epoch-tagger-local</Data>
    <Data Name="UserContext">LAPTOP-4JTHU84J\gombc</Data>
    <Data Name="InstanceId">{5c96b1dd-1d83-494d-9279-6daa391c3073}</Data>
  </EventData>
</Event>

Log Name:      Microsoft-Windows-TaskScheduler/Operational
Source:        Microsoft-Windows-TaskScheduler
Date:          4/15/2026 2:52:30 PM
Event ID:      201
Task Category: Action completed
Level:         Information
Keywords:      
User:          SYSTEM
Computer:      LAPTOP-4JTHU84J
Description:
Task Scheduler successfully completed task "\epoch-tagger-local" , instance "{df11fc63-7b20-4964-8bbe-aa6b8bb231c3}" , action "C:\Windows\System32\cmd.exe" with return code 2147942401.
Event Xml:
<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
  <System>
    <Provider Name="Microsoft-Windows-TaskScheduler" Guid="{de7b24ea-73c8-4a09-985d-5bdadcfa9017}" />
    <EventID>201</EventID>
    <Version>2</Version>
    <Level>4</Level>
    <Task>201</Task>
    <Opcode>2</Opcode>
    <Keywords>0x8000000000000000</Keywords>
    <TimeCreated SystemTime="2026-04-15T18:52:30.6742718Z" />
    <EventRecordID>9965</EventRecordID>
    <Correlation ActivityID="{df11fc63-7b20-4964-8bbe-aa6b8bb231c3}" />
    <Execution ProcessID="3272" ThreadID="19800" />
    <Channel>Microsoft-Windows-TaskScheduler/Operational</Channel>
    <Computer>LAPTOP-4JTHU84J</Computer>
    <Security UserID="S-1-5-18" />
  </System>
  <EventData Name="ActionSuccess">
    <Data Name="TaskName">\epoch-tagger-local</Data>
    <Data Name="TaskInstanceId">{df11fc63-7b20-4964-8bbe-aa6b8bb231c3}</Data>
    <Data Name="ActionName">C:\Windows\System32\cmd.exe</Data>
    <Data Name="ResultCode">2147942401</Data>
    <Data Name="EnginePID">19292</Data>
  </EventData>
</Event>


Log Name:      Microsoft-Windows-TaskScheduler/Operational
Source:        Microsoft-Windows-TaskScheduler
Date:          4/15/2026 2:52:30 PM
Event ID:      102
Task Category: Task completed
Level:         Information
Keywords:      (1)
User:          SYSTEM
Computer:      LAPTOP-4JTHU84J
Description:
Task Scheduler successfully finished "{df11fc63-7b20-4964-8bbe-aa6b8bb231c3}" instance of the "\epoch-tagger-local" task for user "LAPTOP-4JTHU84J\gombc".
Event Xml:
<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
  <System>
    <Provider Name="Microsoft-Windows-TaskScheduler" Guid="{de7b24ea-73c8-4a09-985d-5bdadcfa9017}" />
    <EventID>102</EventID>
    <Version>0</Version>
    <Level>4</Level>
    <Task>102</Task>
    <Opcode>2</Opcode>
    <Keywords>0x8000000000000001</Keywords>
    <TimeCreated SystemTime="2026-04-15T18:52:30.6747953Z" />
    <EventRecordID>9966</EventRecordID>
    <Correlation ActivityID="{df11fc63-7b20-4964-8bbe-aa6b8bb231c3}" />
    <Execution ProcessID="3272" ThreadID="19800" />
    <Channel>Microsoft-Windows-TaskScheduler/Operational</Channel>
    <Computer>LAPTOP-4JTHU84J</Computer>
    <Security UserID="S-1-5-18" />
  </System>
  <EventData Name="TaskSuccessEvent">
    <Data Name="TaskName">\epoch-tagger-local</Data>
    <Data Name="UserContext">LAPTOP-4JTHU84J\gombc</Data>
    <Data Name="InstanceId">{df11fc63-7b20-4964-8bbe-aa6b8bb231c3}</Data>
  </EventData>
</Event>



# inspect
Get-ScheduledTask -TaskName 'epoch-tagger-local' |
  ForEach-Object { $_.Settings } |
  Format-List MultipleInstances, Enabled, ExecutionTimeLimit, Hidden


  # fix repeat behavior
$settings = New-ScheduledTaskSettingsSet `
  -MultipleInstances Queue `
  -ExecutionTimeLimit (New-TimeSpan -Hours 72) `
  -Hidden `
  -Priority 7

Set-ScheduledTask -TaskName 'epoch-tagger-local' -Settings $settings
Enable-ScheduledTask -TaskName 'epoch-tagger-local'


# confirm
Get-ScheduledTask -TaskName 'epoch-tagger-local' |
  ForEach-Object { $_.Settings } |
  Format-List MultipleInstances, Enabled


  Start-Process powershell -Verb RunAs


  $settings = New-ScheduledTaskSettingsSet `
  -MultipleInstances Queue `
  -ExecutionTimeLimit (New-TimeSpan -Hours 72) `
  -Hidden `
  -Priority 7

Set-ScheduledTask -TaskName 'epoch-tagger-local' -Settings $settings
Enable-ScheduledTask -TaskName 'epoch-tagger-local'


$task = Get-ScheduledTask -TaskName 'epoch-tagger-local'

$action    = $task.Actions
$trigger   = $task.Triggers
$principal = $task.Principal
$settings  = New-ScheduledTaskSettingsSet `
  -MultipleInstances Queue `
  -ExecutionTimeLimit (New-TimeSpan -Hours 72) `
  -Hidden `
  -Priority 7

Register-ScheduledTask `
  -TaskName 'epoch-tagger-local' `
  -Action $action `
  -Trigger $trigger `
  -Principal $principal `
  -Settings $settings `
  -Force

  Get-ScheduledTask -TaskName 'epoch-tagger-local' |
  ForEach-Object { $_.Settings } |
  Format-List MultipleInstances, Enabled

notepad "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\run_tagger_task.cmd"
dir "C:\actions-runner\ringstatus\schedules-calculatorv2.log"

  cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"

Get-Content .\run_tagger_task.cmd
Get-Content .\run_tagger_task.ps1
Get-Content .\run_tagger_task_cmd.ps1



  cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"

Select-String -Path .\run_tagger_task.cmd, .\run_tagger_task.ps1, .\run_tagger_task_cmd.ps1 `
  -Pattern 'heartbeat|schedules_dailyv2|schedules_calculatorv2|trips_dailyv2|trips_calculatorv2'



cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"
git status
git pull


  powershell -ExecutionPolicy Bypass -File "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\fix_epoch_tagger_task.ps1"



  Get-ScheduledTask -TaskName 'epoch-tagger-local' |
  Select-Object -ExpandProperty Actions |
  Format-List Execute,Arguments,WorkingDirectory


  Fastest fix:

Open Task Scheduler
epoch-tagger-local
Properties
Actions
Set it to:
Program/script: C:\Windows\System32\cmd.exe
Add arguments: /d /c "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\run_tagger_task.cmd"


<<<<<<< HEAD
dir "C:\actions-runner\ringstatus\schedules-dailyv2.log"
dir "C:\actions-runner\ringstatus\schedules-calculatorv2.log"
=======
Execute          : powershell.exe
Arguments        : -ExecutionPolicy Bypass -File "C:\Users\gombc\OneDrive - Sport Dog
                   Food\github\repos\ringstatus\run_tagger_task.ps1"
WorkingDirectory : C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus

>>>>>>> 4412de39cbfdf4ca3faf3c1c521cdc54101fa296


Get-ScheduledTask -TaskName 'epoch-tagger-local' |
  Select-Object -ExpandProperty Actions |
  Format-List Execute,Arguments,WorkingDirectory


  "C:\Users\gombc\OneDrive - Sport Dog Food"

  
Execute          : C:\Windows\System32\cmd.exe
Arguments        : /d /c "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\run_tagger_task.cmd"
WorkingDirectory :

Error: UNKNOWN: unknown error, open 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\schedules_dailyv2.js'
    at Object.readFileSync (node:fs:440:20)
    at defaultLoadImpl (node:internal/modules/cjs/loader:1148:17)
    at loadSource (node:internal/modules/cjs/loader:1838:20)
    at Object..js (node:internal/modules/cjs/loader:1936:44)
    at Module.load (node:internal/modules/cjs/loader:1533:32)
    at Module._load (node:internal/modules/cjs/loader:1335:12)
    at wrapModuleLoad (node:internal/modules/cjs/loader:255:19)
    at Module.executeUserEntryPoint [as runMain] (node:internal/modules/run_main:154:5)
    at node:internal/main/run_main_module:33:47 {
  errno: -4094,
  code: 'UNKNOWN',
  syscall: 'open',
  path: 'C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\schedules_dailyv2.js'
}

Node.js v24.14.1
[Wed 04/15/2026 14:15:23.35] SCHEDULES_DAILYV2 RUN 
node:fs:440
    return binding.readFileUtf8(path, stringToFlags(options.flag));
                   ^

Error: UNKNOWN: unknown error, open 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\schedules_dailyv2.js'
    at Object.readFileSync (node:fs:440:20)
    at defaultLoadImpl (node:internal/modules/cjs/loader:1148:17)
    at loadSource (node:internal/modules/cjs/loader:1838:20)
    at Object..js (node:internal/modules/cjs/loader:1936:44)
    at Module.load (node:internal/modules/cjs/loader:1533:32)
    at Module._load (node:internal/modules/cjs/loader:1335:12)
    at wrapModuleLoad (node:internal/modules/cjs/loader:255:19)
    at Module.executeUserEntryPoint [as runMain] (node:internal/modules/run_main:154:5)
    at node:internal/main/run_main_module:33:47 {
  errno: -4094,
  code: 'UNKNOWN',
  syscall: 'open',
  path: 'C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\schedules_dailyv2.js'
}

Node.js v24.14.1

attrib +P -U "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus" /S /D
