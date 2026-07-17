import { useCallback, useEffect, useState } from 'react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import FormControl from '@mui/material/FormControl'
import InputLabel from '@mui/material/InputLabel'
import MenuItem from '@mui/material/MenuItem'
import Select from '@mui/material/Select'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import Chip from '@mui/material/Chip'
import { apiGet, apiMutate } from '../api/client'
import { LoadingBlock } from '../components/LoadingBlock'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'

type AiSettings = {
  configured: boolean
  api_key_set: boolean
  api_key_masked: string | null
  model: string
  base_url: string
  encryption_configured: boolean
  suggested_models: string[]
  chat_history_max_lines: number
  knowledge_ready: boolean
}

type SettingsResponse = {
  ok: boolean
  enabled: boolean
  message?: string
  settings?: AiSettings
}

export function AiSettingsPage() {
  const [data, setData] = useState<SettingsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [apiKey, setApiKey] = useState('')
  const [model, setModel] = useState('')
  const [baseUrl, setBaseUrl] = useState('')
  const [customModel, setCustomModel] = useState('')
  const [notice, setNotice] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const res = await apiGet<SettingsResponse>('/api/ai/settings')
      setData(res)
      if (res.settings) {
        setModel(res.settings.model)
        setBaseUrl(res.settings.base_url)
        const suggested = res.settings.suggested_models || []
        if (res.settings.model && !suggested.includes(res.settings.model)) {
          setCustomModel(res.settings.model)
        }
      }
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  const save = async () => {
    setSaving(true)
    setNotice(null)
    try {
      const body: Record<string, string> = {
        model: customModel.trim() || model,
        base_url: baseUrl.trim(),
      }
      if (apiKey.trim()) body.api_key = apiKey.trim()
      const res = await apiMutate<SettingsResponse>('PUT', '/api/ai/settings', body)
      setData(res)
      setApiKey('')
      setNotice('Settings saved. API key is encrypted in Redis and never shown again in full.')
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  const clearKey = async () => {
    setSaving(true)
    setNotice(null)
    try {
      await apiMutate('DELETE', '/api/ai/settings')
      setNotice('API key cleared.')
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Clear failed')
    } finally {
      setSaving(false)
    }
  }

  if (!data && !error) return <LoadingBlock />
  if (error && !data) return <Alert severity="error">{error}</Alert>
  if (data && !data.enabled) {
    return (
      <Box>
        <PageHeader title="AI Settings" subtitle="OpenRouter configuration for the dashboard assistant." />
        <Alert severity="info">{data.message || 'AI assistant is disabled.'}</Alert>
      </Box>
    )
  }

  const s = data?.settings
  const suggested = s?.suggested_models || []
  const modelValue = suggested.includes(model) ? model : '__custom__'

  return (
    <Box>
      <PageHeader
        title="AI Settings"
        subtitle="Configure OpenRouter for the global admin chat. Keys are encrypted at rest in Redis."
      />
      {error ? (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      ) : null}
      {notice ? (
        <Alert severity="success" sx={{ mb: 2 }} onClose={() => setNotice(null)}>
          {notice}
        </Alert>
      ) : null}

      <Stack spacing={2}>
        <SectionCard title="Status">
          <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
            <Chip
              size="small"
              color={s?.encryption_configured ? 'success' : 'warning'}
              label={s?.encryption_configured ? 'Encryption salt set' : 'Set ai_encryption_salt'}
            />
            <Chip
              size="small"
              color={s?.api_key_set ? 'success' : 'default'}
              label={s?.api_key_set ? `API key ${s.api_key_masked}` : 'No API key'}
            />
            <Chip
              size="small"
              color={s?.knowledge_ready ? 'success' : 'warning'}
              label={s?.knowledge_ready ? 'Knowledge corpus ready' : 'Knowledge not synced yet'}
            />
            <Chip size="small" variant="outlined" label={`History cap ${s?.chat_history_max_lines ?? 500}`} />
          </Stack>
          {!s?.encryption_configured ? (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1.5 }}>
              Set <code>config.ai_encryption_salt</code> (or <code>KAFKA_BATCH_AI_ENCRYPTION_SALT</code>) in your
              kafka_batch initializer before saving an API key.
            </Typography>
          ) : null}
        </SectionCard>

        <SectionCard title="OpenRouter">
          <Stack spacing={2}>
            <TextField
              label="API key"
              type="password"
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              placeholder={s?.api_key_set ? 'Leave blank to keep existing key' : 'sk-or-…'}
              fullWidth
              autoComplete="off"
              helperText="Stored encrypted in Redis. Never returned in clear text after save."
            />
            <FormControl fullWidth>
              <InputLabel id="ai-model-label">Model</InputLabel>
              <Select
                labelId="ai-model-label"
                label="Model"
                value={modelValue}
                onChange={(e) => {
                  const v = e.target.value
                  if (v === '__custom__') {
                    setModel('__custom__')
                  } else {
                    setModel(v)
                    setCustomModel('')
                  }
                }}
              >
                {suggested.map((m) => (
                  <MenuItem key={m} value={m}>
                    {m}
                  </MenuItem>
                ))}
                <MenuItem value="__custom__">Custom model id…</MenuItem>
              </Select>
            </FormControl>
            {modelValue === '__custom__' ? (
              <TextField
                label="Custom model"
                value={customModel}
                onChange={(e) => setCustomModel(e.target.value)}
                fullWidth
                placeholder="provider/model-name"
              />
            ) : null}
            <TextField
              label="Base URL"
              value={baseUrl}
              onChange={(e) => setBaseUrl(e.target.value)}
              fullWidth
              helperText="Default https://openrouter.ai/api/v1"
            />
            <Stack direction="row" spacing={1}>
              <Button variant="contained" onClick={() => void save()} disabled={saving}>
                Save
              </Button>
              <Button variant="outlined" color="warning" onClick={() => void clearKey()} disabled={saving || !s?.api_key_set}>
                Clear API key
              </Button>
            </Stack>
          </Stack>
        </SectionCard>
      </Stack>
    </Box>
  )
}
