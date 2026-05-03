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
        'heartbeat_patterns.js'
    ) `
    -Steps $steps
