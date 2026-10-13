param([switch]$Release, [Parameter(ValueFromRemainingArguments=$true)][string[]]$GoArgs)
$ErrorActionPreference = "Stop"
$ClientDir = $PSScriptRoot
$GoOS = (go env GOOS).Trim()
$GoArch = (go env GOARCH).Trim()
if ($GoOS -ne "windows" -or $GoArch -ne "amd64") { throw "Use build.sh for Unix targets; Windows supports amd64." }
$RustTarget = "x86_64-pc-windows-gnu"
rustup target add $RustTarget
if ($LASTEXITCODE -ne 0) { throw "rustup failed" }
$Profile = "debug"
$CargoArgs = @("rustc", "--locked", "--manifest-path", "$ClientDir/native/Cargo.toml", "--target", $RustTarget, "--lib")
if ($Release) { $Profile = "release"; $CargoArgs += "--release" }
$CargoArgs += @("--", "--print", "native-static-libs")
$Log = & cargo @CargoArgs 2>&1
$Code = $LASTEXITCODE
$Log | ForEach-Object { Write-Host $_ }
if ($Code -ne 0) { throw "Native build failed ($Code)" }
$LibDir = "$ClientDir/native/lib/windows-amd64"
New-Item -ItemType Directory -Force $LibDir | Out-Null
Copy-Item "$ClientDir/native/target/$RustTarget/$Profile/libmurmur_go.a" "$LibDir/libmurmur_go.a"
$NativeFlags = (($Log | Where-Object { "$_" -match '^note: native-static-libs: ' } | Select-Object -Last 1) -replace '^note: native-static-libs: ', '')
Set-Content "$LibDir/native-static-libs.txt" $NativeFlags
go run "$ClientDir/internal/buildstamp/main.go" "$LibDir/libmurmur_go.a" "$ClientDir/native_build_stamp.go"
if ($LASTEXITCODE -ne 0) { throw "Build fingerprint generation failed" }
if ($GoArgs.Count -gt 0) {
    if ($GoArgs[0] -eq "test") {
        $FixtureArgs = @("build", "--locked", "--manifest-path", "$ClientDir/native/Cargo.toml", "--target", $RustTarget, "--example", "peer_fixture")
        if ($Release) { $FixtureArgs += "--release" }
        & cargo @FixtureArgs
        if ($LASTEXITCODE -ne 0) { throw "Rust fixture build failed" }
        $env:MURMUR_RUST_FIXTURE="$ClientDir/native/target/$RustTarget/$Profile/examples/peer_fixture.exe"
    }
    Push-Location $ClientDir
    $PreviousFlags = $env:CGO_LDFLAGS
    $PreviousCGO = $env:CGO_ENABLED
    try { $env:CGO_ENABLED="1"; $env:CGO_LDFLAGS="$PreviousFlags $NativeFlags"; & go @GoArgs; if ($LASTEXITCODE -ne 0) { throw "Go command failed" } }
    finally { $env:CGO_LDFLAGS=$PreviousFlags; $env:CGO_ENABLED=$PreviousCGO; Pop-Location }
}
