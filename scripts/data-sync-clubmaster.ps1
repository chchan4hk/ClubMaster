#Requires -Version 5.1
<#
  Sync local .\src to remote 'clubmaster' branch main under repo path src/.
  Uses a throwaway worktree under $env:TEMP (never next to the repo).
  Usage:
    .\scripts\data-sync-clubmaster.ps1              # full mirror of src
    .\scripts\data-sync-clubmaster.ps1 -HtmlOnly    # copy only *.html
#>
param(
    [switch]$HtmlOnly,
    [string]$Remote = 'clubmaster',
    [string]$Branch = 'main'
)

$ErrorActionPreference = 'Stop'
$clubRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$srcRoot = Join-Path $clubRoot 'src'
$wt = Join-Path $env:TEMP ('cmsync-' + [guid]::NewGuid().ToString('N').Substring(0, 12))

function Remove-SyncWorktree {
    param([string]$WorktreePath)
    if (-not $WorktreePath) { return }
    Push-Location $clubRoot
    try {
        git worktree remove $WorktreePath --force 2>$null
    }
    catch { }
    finally {
        Pop-Location
    }
    if (Test-Path -LiteralPath $WorktreePath) {
        Remove-Item -LiteralPath $WorktreePath -Recurse -Force -ErrorAction SilentlyContinue
    }
}

if (-not (Test-Path -LiteralPath $srcRoot)) {
    throw "Source folder not found: $srcRoot"
}

try {
    Set-Location $clubRoot
    git fetch $Remote
    git worktree add $wt "${Remote}/${Branch}"

    $dst = Join-Path $wt 'src'
    New-Item -ItemType Directory -Force -Path $dst | Out-Null

    if ($HtmlOnly) {
        robocopy $srcRoot $dst '*.html' /S /R:2 /W:2 /NFL /NDL /NJH /NJS | Out-Null
        $rc = $LASTEXITCODE
        if ($rc -ge 8) { throw "robocopy failed with exit code $rc" }
    }
    else {
        robocopy $srcRoot $dst /MIR /R:2 /W:2 /XD node_modules .git /NFL /NDL /NJH /NJS | Out-Null
        $rc = $LASTEXITCODE
        if ($rc -ge 8) { throw "robocopy failed with exit code $rc" }
    }

    Set-Location $wt
    if ($HtmlOnly) {
        git add -- ':(glob)src/**/*.html'
    }
    else {
        git add -A
    }

    git diff --cached --quiet
    if ($LASTEXITCODE -eq 0) {
        Write-Host 'Nothing to commit (remote already matches local src).'
    }
    else {
        $msg = if ($HtmlOnly) { 'DataSync: update HTML files under src from workspace' } else { 'DataSync: mirror updated src from workspace' }
        git commit -m $msg
        git push $Remote "HEAD:${Branch}"
    }
}
finally {
    Set-Location $clubRoot
    Remove-SyncWorktree -WorktreePath $wt
}

Write-Host 'Done.'
