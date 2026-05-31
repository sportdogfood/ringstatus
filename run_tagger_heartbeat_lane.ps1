$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'runner_pipeline_common.ps1')

$steps = @(
    @{
        Label = 'TAGGER'
        ScriptName = 'tagger.js'
        LogFileName = 'epoch-tagger.log'
        ContinueOnError = $false
    },
    @{
        Label = 'HEARTBEAT_PATTERNS'
        ScriptName = 'heartbeat_patterns.js'
        LogFileName = 'epoch-tagger.log'
        ContinueOnError = $false
    },
    @{
        Label = 'HEARTBEAT_TASK_CADENCE'
        ScriptName = 'heartbeat_task_cadence.js'
        LogFileName = 'heartbeat-task-cadence.log'
        ContinueOnError = $true
    },
    @{
        Label = 'SLOT_ORCHESTRATOR'
        ScriptName = 'heartbeat_slot_orchestrator.js'
        LogFileName = 'heartbeat-slot-orchestrator.log'
        ContinueOnError = $true
    }
)

Invoke-RunnerPipeline `
    -ScriptRoot $PSScriptRoot `
    -PipelineName 'heartbeat_lane' `
    -SummaryFileName 'runner-pipeline-heartbeat.log' `
    -TrackedFiles @(
        'run_tagger_heartbeat_lane.cmd',
        'run_tagger_heartbeat_lane.ps1',
        'runner_pipeline_common.ps1',
        'tagger.js',
        'heartbeat_patterns.js',
        'heartbeat_task_cadence.js',
        'lib/heartbeat_mode.js',
        'lib/default_show_date_guard.js',
        'lib/watch_schedule_scope_relink.js',
        'lib/watch_trips_scope_relink.js',
        'lib/watch_trips_scope.js',
        'heartbeat_slot_orchestrator.js',
        'live_groups_daily.js',
        'live_rings_daily.js',
        'live_class_detail.js'
    ) `
    -Steps $steps
