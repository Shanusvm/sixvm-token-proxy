# Builds SixVM-Token-Proxy.exe (Windows single executable) into dist\
# Requires Node 22+ and internet for npx (esbuild, postject).
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "1/5  Bundling server -> build\bundle.cjs"
New-Item build -ItemType Directory -Force | Out-Null
npx --yes esbuild server.js --bundle --platform=node --format=cjs --outfile=build/bundle.cjs --log-level=warning
if ($LASTEXITCODE -ne 0) { throw "esbuild failed" }

Write-Host "2/5  Generating single-executable blob"
'{ "main": "build/bundle.cjs", "output": "build/sea-prep.blob", "disableExperimentalSEAWarning": true }' |
  Set-Content -Encoding ascii sea-config.json
node --experimental-sea-config sea-config.json
if ($LASTEXITCODE -ne 0) { throw "SEA blob generation failed" }

Write-Host "3/5  Creating exe from the Node runtime"
New-Item dist -ItemType Directory -Force | Out-Null
Copy-Item (Get-Command node).Source dist\SixVM-Token-Proxy.exe -Force

Write-Host "4/5  Injecting the app into the exe"
npx --yes postject dist\SixVM-Token-Proxy.exe NODE_SEA_BLOB build\sea-prep.blob --sentinel-fuse NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2
if ($LASTEXITCODE -ne 0) { throw "postject failed" }

Write-Host "5/5  Copying pages and docs into dist\"
Copy-Item dashboard.html, setup.html, help.html, doctor.html, compare.html, .env.example, schema.sql dist\ -Force

@"
SixVM Token Proxy
=================
One proxy for Claude, ChatGPT and Gemini: see every token your
AI agents use, and spend less automatically.

HOW TO START
------------
1. Double-click SixVM-Token-Proxy.exe

   * The FIRST time, Windows may show "Windows protected your PC"
     (SmartScreen). This is normal for a new app: click "More info",
     then "Run anyway". It only happens once.

   * Keep the black window open - closing it stops the proxy.

2. On the first run, your browser opens the Setup page by itself.
   (If it doesn't: open http://localhost:8787/setup yourself.)
   Paste your AI API key(s) there - Claude, ChatGPT and/or Gemini.

3. Point your AI agents at   http://localhost:8787
   Full guide with examples: http://localhost:8787/help

GOOD TO KNOW
------------
* Privacy: your keys and your usage data stay on THIS computer.
  Nothing is sent to SixVM or anyone else. Optionally you can
  connect your own free Supabase database (see the Setup page).
* Your keys are saved in the .env file next to the exe.
  Your usage history is in the data folder next to the exe.
* The proxy only accepts connections from this computer.
* Started it twice? The second window says "already running",
  opens the dashboard, and closes itself. No harm done.
* To stop: close the window.  To uninstall: delete this folder.

(c) SixVM IT Solutions - MIT license
"@ | Set-Content -Encoding ascii dist\README.txt

$size = [math]::Round((Get-Item dist\SixVM-Token-Proxy.exe).Length / 1MB, 1)
Write-Host ""
Write-Host "Done -> dist\SixVM-Token-Proxy.exe ($size MB)"
