$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'runner_pipeline_common.ps1')

$steps = @(
    @{
        Label = 'SCHEDULES_DAILYV2'
        ScriptName = 'schedules_dailyv2.js'
        LogFileName = 'schedules-dailyv2.log'
        ContinueOnError = $true
    },
    @{
        Label = 'SCHEDULES_CALCULATORV2'
        ScriptName = 'schedules_calculatorv2.js'
        LogFileName = 'schedules-calculatorv2.log'
        ContinueOnError = $true
    },
    @{
        Label = 'TRIPS_DAILYV2'
        ScriptName = 'trips_dailyv2.js'
        LogFileName = 'trips-dailyv2.log'
        ContinueOnError = $true
    }
)

Invoke-RunnerPipeline `
    -ScriptRoot $PSScriptRoot `
    -PipelineName 'heavy_lane' `
    -SummaryFileName 'runner-pipeline-heavy.log' `
    -TrackedFiles @(
        'run_tagger_heavy_lane.cmd',
        'run_tagger_heavy_lane.ps1',
        'runner_pipeline_common.ps1',
        'schedules_dailyv2.js',
        'schedules_calculatorv2.js',
        'trips_dailyv2.js'
    ) `
    -Steps $steps
