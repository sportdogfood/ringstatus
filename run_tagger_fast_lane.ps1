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
        Label = 'TRIPS_TAGGER'
        ScriptName = 'trips_tagger.js'
        LogFileName = 'trips-tagger.log'
        ContinueOnError = $false
    },
    @{
        Label = 'TRIPS_CALCULATORV2'
        ScriptName = 'trips_calculatorv2.js'
        LogFileName = 'trips-calculatorv2.log'
        ContinueOnError = $true
    },
    @{
        Label = 'PUBLISH'
        ScriptName = 'publisher.js'
        LogFileName = 'publisher.log'
        ContinueOnError = $false
    }
)

Invoke-RunnerPipeline `
    -ScriptRoot $PSScriptRoot `
    -PipelineName 'fast_lane' `
    -SummaryFileName 'runner-pipeline-fast.log' `
    -TrackedFiles @(
        'run_tagger_fast_lane.cmd',
        'run_tagger_fast_lane.ps1',
        'runner_pipeline_common.ps1',
        'tagger.js',
        'heartbeat_patterns.js',
        'trips_tagger.js',
        'trips_calculatorv2.js',
        'trips_calculator.js',
        'publisher.js'
    ) `
    -Steps $steps `
    -IncludePublisherDelay
