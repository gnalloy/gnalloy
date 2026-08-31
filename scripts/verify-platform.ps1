param(
    [switch]$SkipCrossCompile,
    [switch]$SkipBench,
    [string]$MatrixPath,
    [string]$ReportPath
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($MatrixPath)) {
    $MatrixPath = Join-Path $PSScriptRoot "platform-matrix.json"
}

$oldGoWork = $env:GOWORK
$oldGoOS = $env:GOOS
$oldGoArch = $env:GOARCH
$results = New-Object System.Collections.Generic.List[object]
$failed = $false

function Add-Result {
    param(
        [string]$Name,
        [string]$Target,
        [string]$Status,
        [string]$Reason,
        [double]$DurationMs
    )
    $results.Add([pscustomobject]@{
        name = $Name
        target = $Target
        status = $Status
        reason = $Reason
        durationMs = [math]::Round($DurationMs, 2)
    }) | Out-Null
}

function Invoke-NativeChecked {
    param(
        [string]$FilePath,
        [string[]]$Arguments
    )
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath exited with code $LASTEXITCODE"
    }
}

function Invoke-Gate {
    param(
        [string]$Name,
        [string]$Target,
        [scriptblock]$Action
    )
    $started = Get-Date
    Write-Host "== $Target/$Name"
    try {
        & $Action
        $duration = ((Get-Date) - $started).TotalMilliseconds
        Add-Result -Name $Name -Target $Target -Status "passed" -Reason "" -DurationMs $duration
    } catch {
        $script:failed = $true
        $duration = ((Get-Date) - $started).TotalMilliseconds
        Add-Result -Name $Name -Target $Target -Status "failed" -Reason $_.Exception.Message -DurationMs $duration
        Write-Host "FAILED ${Target}/${Name}: $($_.Exception.Message)"
    }
}

function Add-SkippedGate {
    param(
        [string]$Name,
        [string]$Target,
        [string]$Reason
    )
    Write-Host "== $Target/$Name skipped: $Reason"
    Add-Result -Name $Name -Target $Target -Status "skipped" -Reason $Reason -DurationMs 0
}

function Test-AllowedSyscallMatch {
    param([string]$Line)
    $path = ($Line -split ":", 2)[0]
    $path = $path.TrimStart(".", "\", "/").Replace("/", "\")
    return $path -in @(
        "transport\tcp\socket_windows.go",
        "transport\tcp\socket_windows_connect.go"
    )
}

Push-Location $repo
try {
    if (-not (Test-Path -LiteralPath $MatrixPath)) {
        throw "matrix not found: $MatrixPath"
    }
    $matrix = Get-Content -LiteralPath $MatrixPath -Raw | ConvertFrom-Json
    $env:GOWORK = "off"
    $nativeGOOS = (& go env GOOS).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "go env GOOS failed"
    }
    $nativeGOARCH = (& go env GOARCH).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "go env GOARCH failed"
    }
    $nativeTarget = "$nativeGOOS/$nativeGOARCH"

    Invoke-Gate -Name "native-tests" -Target $nativeTarget -Action {
        Invoke-NativeChecked -FilePath "go" -Arguments @("test", "./...")
    }

    if ($SkipCrossCompile) {
        foreach ($target in $matrix.targets) {
            Add-SkippedGate -Name "cross-compile" -Target "$($target.goos)/$($target.goarch)" -Reason "SkipCrossCompile"
        }
    } else {
        foreach ($target in $matrix.targets) {
            $targetName = "$($target.goos)/$($target.goarch)"
            Invoke-Gate -Name "cross-compile" -Target $targetName -Action {
                $env:GOOS = $target.goos
                $env:GOARCH = $target.goarch
                $packages = & go list ./...
                if ($LASTEXITCODE -ne 0) {
                    throw "go list failed for $targetName"
                }
                foreach ($package in $packages) {
                    Invoke-NativeChecked -FilePath "go" -Arguments @("test", "-c", "-o", "NUL", $package)
                }
            }
        }
        Remove-Item Env:\GOOS -ErrorAction SilentlyContinue
        Remove-Item Env:\GOARCH -ErrorAction SilentlyContinue
    }

    if ($SkipBench) {
        Add-SkippedGate -Name "benchmarks" -Target $nativeTarget -Reason "SkipBench"
    } else {
        Invoke-Gate -Name "benchmarks" -Target $nativeTarget -Action {
            foreach ($package in @("./buffer", "./channel", "./codec", "./queue", "./timer", "./transport/tcp")) {
                Invoke-NativeChecked -FilePath "go" -Arguments @("test", "-run", "^$", "-bench", ".", "-benchmem", $package)
            }
        }
    }

    Invoke-Gate -Name "source-scan" -Target "all" -Action {
        if (Get-Command rg -ErrorAction SilentlyContinue) {
            $matches = & rg -n '^\s*"syscall"\s*$|\bsyscall\.' .
            if ($LASTEXITCODE -eq 0) {
                $violations = @()
                foreach ($match in $matches) {
                    if (-not (Test-AllowedSyscallMatch -Line $match)) {
                        $violations += $match
                    }
                }
                if ($violations.Count -gt 0) {
                    $violations | ForEach-Object { Write-Host $_ }
                    throw "direct syscall usage found outside allowlist; use golang.org/x/sys instead"
                }
                Write-Host "legacy syscall allowlist matched only Windows TCP socket shim files"
            }
            if ($LASTEXITCODE -gt 1) {
                throw "rg source scan failed"
            }
        } else {
            $matches = Get-ChildItem -Recurse -Filter *.go | Select-String -Pattern '^\s*"syscall"\s*$|\bsyscall\.'
            $violations = @()
            foreach ($match in $matches) {
                $relative = Resolve-Path -LiteralPath $match.Path -Relative
                if (-not (Test-AllowedSyscallMatch -Line "${relative}:$($match.LineNumber):$($match.Line)")) {
                    $violations += $match
                }
            }
            if ($violations.Count -gt 0) {
                $violations | ForEach-Object { Write-Host $_.ToString() }
                throw "direct syscall usage found outside allowlist; use golang.org/x/sys instead"
            }
        }
    }
} finally {
    if ($null -eq $oldGoWork) {
        Remove-Item Env:\GOWORK -ErrorAction SilentlyContinue
    } else {
        $env:GOWORK = $oldGoWork
    }
    if ($null -eq $oldGoOS) {
        Remove-Item Env:\GOOS -ErrorAction SilentlyContinue
    } else {
        $env:GOOS = $oldGoOS
    }
    if ($null -eq $oldGoArch) {
        Remove-Item Env:\GOARCH -ErrorAction SilentlyContinue
    } else {
        $env:GOARCH = $oldGoArch
    }
    if (-not [string]::IsNullOrWhiteSpace($ReportPath)) {
        $report = [pscustomobject]@{
            matrix = (Resolve-Path -LiteralPath $MatrixPath).Path
            generatedAt = (Get-Date).ToString("o")
            results = $results
        }
        $report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $ReportPath -Encoding utf8
    }
    Pop-Location
}

if ($failed) {
    throw "platform verification failed"
}
