param(
    [string]$Benchtime = "100ms",
    [int]$Count = 1,
    [switch]$SkipBench
)

$ErrorActionPreference = "Stop"
$oldGoWork = $env:GOWORK
$oldGoToolchain = $env:GOTOOLCHAIN
try {
    $env:GOWORK = "off"
    $env:GOTOOLCHAIN = "local"

    $formatted = gofmt -l .
    if (-not [string]::IsNullOrWhiteSpace($formatted)) {
        throw "gofmt required:`n$formatted"
    }

    go test ./... -count=$Count
    if ($LASTEXITCODE -ne 0) { throw "go test failed" }

    go vet ./...
    if ($LASTEXITCODE -ne 0) { throw "go vet failed" }

    if (-not $SkipBench) {
        go test ./... -run '^$' -bench . -benchmem -benchtime=$Benchtime -count=1
        if ($LASTEXITCODE -ne 0) { throw "benchmark smoke failed" }
    }
} finally {
    if ($null -eq $oldGoWork) { Remove-Item Env:\GOWORK -ErrorAction SilentlyContinue } else { $env:GOWORK = $oldGoWork }
    if ($null -eq $oldGoToolchain) { Remove-Item Env:\GOTOOLCHAIN -ErrorAction SilentlyContinue } else { $env:GOTOOLCHAIN = $oldGoToolchain }
}