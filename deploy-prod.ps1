param(
  [switch]$DryRun,
  [switch]$SkipSecrets,
  [switch]$SkipPubSub
)

$ErrorActionPreference = "Stop"

function Run([string]$cmd) {
  Write-Host ">>> $cmd" -ForegroundColor Cyan
  if (-not $DryRun) { Invoke-Expression $cmd }
}

# Robust existence check
function SecretExists([string]$name, [string]$project) {
  $cmd = "gcloud.cmd secrets describe $name --project $project --format=""value(name)"""
  Write-Host ">>> $cmd" -ForegroundColor Cyan
  if ($DryRun) { return $true }
  $old = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
  $null = & cmd /c $cmd 2>&1
  $code = $LASTEXITCODE; $ErrorActionPreference = $old
  return ($code -eq 0)
}

function TopicExists([string]$name, [string]$project) {
  $cmd = "gcloud.cmd pubsub topics describe $name --project $project --format=""value(name)"""
  Write-Host ">>> $cmd" -ForegroundColor Cyan
  if ($DryRun) { return $true }
  $old = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
  $null = & cmd /c $cmd 2>&1
  $code = $LASTEXITCODE; $ErrorActionPreference = $old
  return ($code -eq 0)
}

function SubscriptionExists([string]$name, [string]$project) {
  $cmd = "gcloud.cmd pubsub subscriptions describe $name --project $project --format=""value(name)"""
  Write-Host ">>> $cmd" -ForegroundColor Cyan
  if ($DryRun) { return $true }
  $old = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
  $null = & cmd /c $cmd 2>&1
  $code = $LASTEXITCODE; $ErrorActionPreference = $old
  return ($code -eq 0)
}


function Remove-BOM([string]$s) {
  if ($null -eq $s) { return $s }
  $s = $s -replace "^\uFEFF", ""
  return $s.Trim()
}
function New-SecretTempFile([string]$value) {
  $tmp = New-TemporaryFile
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllText($tmp, $value, $utf8NoBom)
  return $tmp
}

function Save-SecretWithValue([string]$name, [string]$value, [string]$project, [string]$saToGrant) {
  if ($DryRun) { return }
  $tmp = New-SecretTempFile -value $value
  if (-not (SecretExists -name $name -project $project)) {
    Run "gcloud secrets create $name --project $project --replication-policy=automatic --data-file=""$tmp"""
  } else {
    Run "gcloud secrets versions add $name --project $project --data-file=""$tmp"""
  }
  if ($saToGrant) {
    Run "gcloud secrets add-iam-policy-binding $name --project $project --member=""serviceAccount:$saToGrant"" --role=""roles/secretmanager.secretAccessor"" --quiet"
  }
}

function Get-RunUrl([string]$svc, [string]$project, [string]$region) {
  $echo = "gcloud run services describe $svc --project $project --region $region --format=""value(status.url)"""
  Write-Host ">>> $echo" -ForegroundColor Cyan
  if ($DryRun) { return "https://$svc-placeholder.a.run.app" }

  $url = & gcloud run services describe $svc --project $project --region $region --format="value(status.url)" 2>$null

  if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($url)) {
    Write-Warning "Could not resolve Cloud Run URL for service '$svc' in region '$region' (project '$project')."
    return $null
  }
  return $url.Trim()
}
function Set-PubSubTopic([string]$topic, [string]$project, [string]$retention) {
  if (-not (TopicExists -name $topic -project $project)) {
    Write-Host "Creating topic $topic" -ForegroundColor Yellow
    $create = "gcloud pubsub topics create $topic --project $project"
    if ($retention) { $create += " --message-retention-duration=$retention" }
    Run $create
  } else {
    if ($retention) {
      Write-Host "Updating retention on $topic → $retention" -ForegroundColor Yellow
      Run "gcloud pubsub topics update $topic --project $project --message-retention-duration=$retention"
    } else {
      Write-Host "Topic $topic exists" -ForegroundColor Gray
    }
  }
}
function Set-PubSubSubscription(
  [string]$subName, [string]$topic, [string]$project,
  [string]$pushEndpoint, [string]$oidcSA,
  [string]$dlqTopic, [string]$maxAttempts, [bool]$ordering
) {
  if (SubscriptionExists -name $subName -project $project) {
    Write-Host "Subscription $subName exists" -ForegroundColor Gray
    return
  }
  $cmd = @(
    "gcloud pubsub subscriptions create $subName",
    "--project $project",
    "--topic=$topic"
  )
  if ($pushEndpoint) { $cmd += "--push-endpoint=""$pushEndpoint""" }
  if ($oidcSA)      { $cmd += "--push-auth-service-account=""$oidcSA""" }
  if ($dlqTopic)    { $cmd += "--dead-letter-topic=projects/$project/topics/$dlqTopic" }
  if ($maxAttempts) { $cmd += "--max-delivery-attempts=$maxAttempts" }
  if ($ordering)    { $cmd += "--enable-message-ordering" }

  Run ($cmd -join " ")
}
function Add-RunInvokerBinding([string]$service, [string]$member, [string]$project, [string]$region) {
  Write-Host "Granting run.invoker to $member on service $service" -ForegroundColor Yellow
  Run ("gcloud run services add-iam-policy-binding {0} --project {1} --region {2} --member serviceAccount:{3} --role roles/run.invoker" `
       -f $service, $project, $region, $member)
}

# --- Load .env into a map ---
$envMap = @{}
$envFile = ".env"
if (!(Test-Path $envFile)) { throw ".env not found" }

(Get-Content $envFile | Where-Object { $_ -and ($_ -notmatch '^\s*#') }) | ForEach-Object {
  $line = Remove-BOM $_
  $pair = $line -split '=', 2
  if ($pair.Count -eq 2) {
    $key = Remove-BOM ($pair[0].Trim())
    $val = Remove-BOM ($pair[1].Trim())
    if (($val.StartsWith('"') -and $val.EndsWith('"')) -or ($val.StartsWith("'") -and $val.EndsWith("'"))) {
      $val = $val.Substring(1, $val.Length - 2)
    }
    $envMap[$key] = $val
  }
}

# Runtime envs for parser (as secrets)
$runtimeKeys = @(
  'DB_USER','DB_PASSWORD','DB_NAME','INSTANCE_CONNECTION_NAME','PRIVATE_IP',
  'HTTPPORT','CALLBACK_URL',
  'GCP_PROJECT_ID','CALLBACK_TOPIC'
) | Where-Object { $envMap.ContainsKey($_) -and -not [string]::IsNullOrWhiteSpace($envMap[$_]) }

if ($runtimeKeys.Count -eq 0) { throw "No runtime variables found in .env for parser." }

# --- Deploy params ---
$serviceName = "ble-parser"

$projectId = (& gcloud config get-value project) -replace '\s',''
if (-not $projectId) { throw "Set default project first: gcloud config set project <ID>" }

$region = (& gcloud config get-value run/region) -replace '\s',''
if (-not $region) { $region = "europe-west1" }

# Ensure CALLBACK_TOPIC and GCP_PROJECT_ID defaults
if (-not $envMap.ContainsKey('CALLBACK_TOPIC') -or [string]::IsNullOrWhiteSpace($envMap['CALLBACK_TOPIC'])) {
  $envMap['CALLBACK_TOPIC'] = 'ble-callbacks'
}
if (-not $envMap.ContainsKey('GCP_PROJECT_ID') -or [string]::IsNullOrWhiteSpace($envMap['GCP_PROJECT_ID'])) {
  $envMap['GCP_PROJECT_ID'] = $projectId
}
foreach ($k in @('CALLBACK_TOPIC','GCP_PROJECT_ID')) {
  if (-not ($runtimeKeys -contains $k)) { $runtimeKeys += $k }
}

$repo = "$region-docker.pkg.dev/$projectId/e2blebackend"
$imageTag = if ($envMap.ContainsKey('IMAGE_TAG') -and $envMap.IMAGE_TAG) { $envMap.IMAGE_TAG } else { "latest" }
$imagePath = "{0}/{1}:{2}" -f $repo, $serviceName, $imageTag

$svcAcct = if ($envMap.ContainsKey('SERVICE_ACCOUNT') -and $envMap.SERVICE_ACCOUNT) {
  $envMap.SERVICE_ACCOUNT
} else {
  "$serviceName-cloudrun@$projectId.iam.gserviceaccount.com"
}

$sqlInst = $envMap['INSTANCE_CONNECTION_NAME']

Write-Host "Project: $projectId" -ForegroundColor Gray
Write-Host "Region:  $region" -ForegroundColor Gray
Write-Host "Image:   $imagePath" -ForegroundColor Gray
Write-Host "SA:      $svcAcct" -ForegroundColor Gray

# ---------- Pub/Sub configuration ----------
if (-not $SkipPubSub) {
  # Defaults
  $topic        = 'ble-callbacks'
  $dlqTopic     = $null
  $subName      = 'ble-callbacks-push'
  $retention    = '7d'
  $maxAttempts  = '10'
  $orderingFlag = $false

  # Overrides
  if ($envMap.ContainsKey('CALLBACK_TOPIC') -and $envMap['CALLBACK_TOPIC']) { $topic = $envMap['CALLBACK_TOPIC'] }
  if ($envMap.ContainsKey('CALLBACK_DLQ_TOPIC') -and $envMap['CALLBACK_DLQ_TOPIC']) { $dlqTopic = $envMap['CALLBACK_DLQ_TOPIC'] } else { $dlqTopic = "$topic-dlq" }
  if ($envMap.ContainsKey('CALLBACK_SUBSCRIPTION_NAME') -and $envMap['CALLBACK_SUBSCRIPTION_NAME']) { $subName = $envMap['CALLBACK_SUBSCRIPTION_NAME'] }
  if ($envMap.ContainsKey('PUBSUB_RETENTION') -and $envMap['PUBSUB_RETENTION']) { $retention = $envMap['PUBSUB_RETENTION'] }
  if ($envMap.ContainsKey('PUBSUB_MAX_DELIVERY_ATTEMPTS') -and $envMap['PUBSUB_MAX_DELIVERY_ATTEMPTS']) { $maxAttempts = $envMap['PUBSUB_MAX_DELIVERY_ATTEMPTS'] }
  if ($envMap.ContainsKey('CALLBACK_ORDERING') -and $envMap['CALLBACK_ORDERING']) {
    $val = $envMap['CALLBACK_ORDERING'].ToString().ToLower()
    if ($val -eq '1' -or $val -eq 'true' -or $val -eq 'yes') { $orderingFlag = $true }
  }

  # Topics
  Set-PubSubTopic -topic $topic    -project $projectId -retention $retention
  Set-PubSubTopic -topic $dlqTopic -project $projectId -retention $retention

  # Publisher grant to parser SA
  Write-Host "Granting publisher on topic $topic to $svcAcct" -ForegroundColor Yellow
  Run "gcloud pubsub topics add-iam-policy-binding $topic --project $projectId --member=""serviceAccount:$svcAcct"" --role=""roles/pubsub.publisher"""

  # Push endpoint
  $pushEndpoint = $null
  if ($envMap.ContainsKey('CALLBACK_PUSH_ENDPOINT') -and $envMap['CALLBACK_PUSH_ENDPOINT']) { $pushEndpoint = $envMap['CALLBACK_PUSH_ENDPOINT'] }
  $callbackRunService = $null
  if ($envMap.ContainsKey('CALLBACK_RUN_SERVICE') -and $envMap['CALLBACK_RUN_SERVICE']) { $callbackRunService = $envMap['CALLBACK_RUN_SERVICE'] }
  if (-not $pushEndpoint -and $callbackRunService) { $pushEndpoint = Get-RunUrl -svc $callbackRunService -project $projectId -region $region }

  # Push auth SA (explicit or default)
  $pushAuthSA = $null
  if ($envMap.ContainsKey('CALLBACK_PUSH_SA') -and $envMap['CALLBACK_PUSH_SA']) { $pushAuthSA = $envMap['CALLBACK_PUSH_SA'] } else { $pushAuthSA = "ble-pubsub-push@$projectId.iam.gserviceaccount.com" }

  # Pub/Sub service agent
  $projectNumber = (& gcloud projects describe $projectId --format='value(projectNumber)').Trim()
  $pubsubSA = "service-$projectNumber@gcp-sa-pubsub.iam.gserviceaccount.com"

  # Ensure push SA exists
  $existsPush = (& gcloud iam service-accounts describe $pushAuthSA --project $projectId --format="value(email)" 2>$null)
  if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($existsPush)) {
    $pushSaId = $pushAuthSA.Split('@')[0]
    Write-Host "Creating push service account $pushAuthSA" -ForegroundColor Yellow
    Run "gcloud iam service-accounts create $pushSaId --project $projectId --display-name ""Pub/Sub Push (callbacks)"""
  }

  # Allow Pub/Sub SA to mint OIDC as push SA
  Run "gcloud iam service-accounts add-iam-policy-binding $pushAuthSA --project $projectId --member=""serviceAccount:$pubsubSA"" --role=""roles/iam.serviceAccountTokenCreator"""

  # Allow current caller to ActAs push SA (to set --push-auth-service-account)
  $caller = (& gcloud config get-value account) -replace '^\s+|\s+$',''
  if ($caller) {
    if ($caller.ToLower().Contains("gserviceaccount.com")) { $callerMember = "serviceAccount:$caller" } else { $callerMember = "user:$caller" }
    Run "gcloud iam service-accounts add-iam-policy-binding $pushAuthSA --project $projectId --member ""$callerMember"" --role roles/iam.serviceAccountUser"
  }

  # Bind run.invoker for push SA on the callback service
  if ($callbackRunService -and $pushEndpoint) {
    Add-RunInvokerBinding -service $callbackRunService -member $pushAuthSA -project $projectId -region $region
  } elseif ($callbackRunService -and -not $pushEndpoint) {
    Write-Warning "CALLBACK_RUN_SERVICE is set to '$callbackRunService' but the service URL could not be resolved. Skipping invoker binding + subscription creation."
  }

  # Create push subscription
  if ($pushEndpoint) {
    Set-PubSubSubscription -subName $subName -topic $topic -project $projectId `
      -pushEndpoint $pushEndpoint -oidcSA $pushAuthSA `
      -dlqTopic $dlqTopic -maxAttempts $maxAttempts -ordering $orderingFlag
  }
} else {
  Write-Host "Skipping Pub/Sub configuration per -SkipPubSub." -ForegroundColor Yellow
}

# ---------- Secrets: create/rotate unless -SkipSecrets ----------
$updatePairs = @()

# Ensure CALLBACK_TOPIC secret exists (with default) when not skipping secrets
if (-not $SkipSecrets) {
  $cbTopicValue = ''
  if ($envMap.ContainsKey('CALLBACK_TOPIC') -and -not [string]::IsNullOrWhiteSpace($envMap['CALLBACK_TOPIC'])) { $cbTopicValue = $envMap['CALLBACK_TOPIC'] } else { $cbTopicValue = 'ble-callbacks' }
  Save-SecretWithValue -name "CALLBACK_TOPIC" -value $cbTopicValue -project $projectId -saToGrant $svcAcct
  if (-not ($runtimeKeys -contains 'CALLBACK_TOPIC')) { $runtimeKeys += 'CALLBACK_TOPIC' }
}

foreach ($key in $runtimeKeys) {
  # If skipping secrets, only include pairs for secrets that already exist
  if ($SkipSecrets -and -not (SecretExists -name $key -project $projectId)) {
    Write-Warning "Skipping $key in --update-secrets (secret not found and -SkipSecrets is set)."
    continue
  }

  $updatePairs += "{0}={1}:latest" -f $key, $key

  if ($SkipSecrets) { continue }

  $val = Remove-BOM $envMap[$key]
  $tmpFile = New-SecretTempFile -value $val

  $exists = SecretExists -name $key -project $projectId
  if (-not $exists) {
    Write-Host "Creating secret $key" -ForegroundColor Yellow
    Run "gcloud secrets create $key --project $projectId --replication-policy=automatic --data-file=""$tmpFile"""
  } else {
    Write-Host "Adding new version to secret $key" -ForegroundColor Yellow
    Run "gcloud secrets versions add $key --project $projectId --data-file=""$tmpFile"""
  }

  Run "gcloud secrets add-iam-policy-binding $key --project $projectId --member=""serviceAccount:$svcAcct"" --role=""roles/secretmanager.secretAccessor"" --quiet"
}

$updateSecretsArg = ($updatePairs -join ",")

# ---------- Build & Deploy ble-parser ----------
$cmd = @(
  "gcloud run deploy $serviceName",
  "--project ""$projectId""",
  "--image=""$imagePath""",
  "--platform managed",
  "--region $region",
  "--service-account=""$svcAcct""",
  "--allow-unauthenticated",
  "--update-secrets=""$updateSecretsArg"""
)

if ($sqlInst -and $sqlInst.Trim() -ne "") { $cmd += "--add-cloudsql-instances=""$sqlInst""" }

foreach ($kv in @('CR_MEMORY','CR_CPU','CR_MIN_INSTANCES','CR_MAX_INSTANCES','CR_CONCURRENCY','VPC_CONNECTOR')) {
  if ($envMap.ContainsKey($kv) -and $envMap[$kv]) {
    switch ($kv) {
      'CR_MEMORY'        { $cmd += "--memory=$($envMap[$kv])" }
      'CR_CPU'           { $cmd += "--cpu=$($envMap[$kv])" }
      'CR_MIN_INSTANCES' { $cmd += "--min-instances=$($envMap[$kv])" }
      'CR_MAX_INSTANCES' { $cmd += "--max-instances=$($envMap[$kv])" }
      'CR_CONCURRENCY'   { $cmd += "--concurrency=$($envMap[$kv])" }
      'VPC_CONNECTOR'    { $cmd += "--vpc-connector=$($envMap[$kv])" }
    }
  }
}

$deployCommand = ($cmd -join " ")
Write-Host "`nDeploy command:" -ForegroundColor Yellow
Write-Host $deployCommand -ForegroundColor Cyan

if ($DryRun) {
  Write-Host "`n(Dry run) Not executing." -ForegroundColor Yellow
  return
}

Run $deployCommand
Write-Host "`nDeployed parser; Pub/Sub configured (unless skipped); UTF-8 no-BOM secrets set." -ForegroundColor Green