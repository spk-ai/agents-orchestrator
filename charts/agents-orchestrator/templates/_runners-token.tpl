{{- /*
  Mounts the projected Runners caller token and points RUNNERS_TOKEN_FILE at
  it (see internal/runnerscreds). Only the Runners connection sends it.
*/ -}}
{{- define "agents-orchestrator.configureRunnersToken" -}}
{{- $token := default dict .Values.runnersCallerToken -}}
{{- if $token.enabled -}}
{{- $audience := trimAll " \n\t" (default "" $token.audience) -}}
{{- if not $audience -}}
{{- fail "runnersCallerToken.audience is required when enabled" -}}
{{- end -}}
{{- $expiration := int (default 600 $token.expirationSeconds) -}}
{{- if lt $expiration 600 -}}
{{- fail "runnersCallerToken.expirationSeconds must be at least 600" -}}
{{- end -}}
{{- $mountPath := trimSuffix "/" (default "/var/run/secrets/agyn.io/runners-token" $token.mountPath) -}}
{{- $volume := dict "name" "runners-caller-token" "projected" (dict "sources" (list (dict "serviceAccountToken" (dict "audience" $audience "expirationSeconds" $expiration "path" "token")))) -}}
{{- $mount := dict "name" "runners-caller-token" "mountPath" $mountPath "readOnly" true -}}
{{- $env := dict "name" "RUNNERS_TOKEN_FILE" "value" (printf "%s/token" $mountPath) -}}
{{- $_ := set .Values "extraVolumes" (append (.Values.extraVolumes | default (list)) $volume) -}}
{{- $_ := set .Values "extraVolumeMounts" (append (.Values.extraVolumeMounts | default (list)) $mount) -}}
{{- $_ := set .Values "env" (append (.Values.env | default (list)) $env) -}}
{{- end -}}
{{- end -}}
