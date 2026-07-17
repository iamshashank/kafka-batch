import { useCallback, useEffect, useRef, useState } from 'react'
import { Link as RouterLink } from 'react-router-dom'
import Box from '@mui/material/Box'
import Fab from '@mui/material/Fab'
import Paper from '@mui/material/Paper'
import IconButton from '@mui/material/IconButton'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import Button from '@mui/material/Button'
import Divider from '@mui/material/Divider'
import CircularProgress from '@mui/material/CircularProgress'
import Tooltip from '@mui/material/Tooltip'
import Link from '@mui/material/Link'
import ChatOutlinedIcon from '@mui/icons-material/ChatOutlined'
import CloseIcon from '@mui/icons-material/Close'
import SendIcon from '@mui/icons-material/Send'
import DeleteOutlinedIcon from '@mui/icons-material/DeleteOutlined'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import { apiGet, apiMutate } from '../api/client'

type ChatMessage = {
  id?: string
  role: string
  content: string
  at?: string
}

type HistoryResponse = {
  ok: boolean
  messages: ChatMessage[]
  size: number
  max_lines: number
}

type ChatResponse = {
  ok: boolean
  reply: string
  model?: string
  history_size?: number
  error?: string
}

export function ChatBubble({ enabled }: { enabled: boolean }) {
  const [open, setOpen] = useState(false)
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [maxLines, setMaxLines] = useState(500)
  const bottomRef = useRef<HTMLDivElement | null>(null)

  const loadHistory = useCallback(async () => {
    try {
      const res = await apiGet<HistoryResponse>('/api/ai/history')
      setMessages(res.messages || [])
      setMaxLines(res.max_lines || 500)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load history')
    }
  }, [])

  useEffect(() => {
    if (open && enabled) void loadHistory()
  }, [open, enabled, loadHistory])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, open])

  if (!enabled) return null

  const send = async () => {
    const text = input.trim()
    if (!text || loading) return
    setInput('')
    setLoading(true)
    setError(null)
    setMessages((prev) => [...prev, { role: 'user', content: text, at: new Date().toISOString() }])
    try {
      const res = await apiMutate<ChatResponse>('POST', '/api/ai/chat', { message: text })
      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          content: res.reply,
          at: new Date().toISOString(),
        },
      ])
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Chat failed'
      setError(msg)
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: `Error: ${msg}`, at: new Date().toISOString() },
      ])
    } finally {
      setLoading(false)
    }
  }

  const clearHistory = async () => {
    try {
      await apiMutate('DELETE', '/api/ai/history')
      setMessages([])
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Clear failed')
    }
  }

  return (
    <>
      <Fab
        color="primary"
        aria-label="Open AI chat"
        onClick={() => setOpen(true)}
        sx={{
          position: 'fixed',
          right: 24,
          bottom: 24,
          zIndex: (t) => t.zIndex.snackbar,
          display: open ? 'none' : 'flex',
        }}
      >
        <ChatOutlinedIcon />
      </Fab>

      {open ? (
        <Paper
          elevation={8}
          sx={{
            position: 'fixed',
            right: { xs: 8, sm: 24 },
            bottom: { xs: 8, sm: 24 },
            width: { xs: 'calc(100vw - 16px)', sm: 420 },
            height: { xs: 'min(70vh, 560px)', sm: 560 },
            zIndex: (t) => t.zIndex.modal,
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
            borderRadius: 2,
          }}
        >
          <Stack
            direction="row"
            alignItems="center"
            spacing={1}
            sx={{ px: 1.5, py: 1, borderBottom: 1, borderColor: 'divider' }}
          >
            <Typography variant="subtitle2" sx={{ flex: 1 }}>
              kafka-batch assistant
            </Typography>
            <Tooltip title="AI Settings">
              <IconButton size="small" component={RouterLink} to="/ai" onClick={() => setOpen(false)}>
                <SettingsOutlinedIcon fontSize="small" />
              </IconButton>
            </Tooltip>
            <Tooltip title="Clear global history">
              <IconButton size="small" onClick={() => void clearHistory()}>
                <DeleteOutlinedIcon fontSize="small" />
              </IconButton>
            </Tooltip>
            <IconButton size="small" onClick={() => setOpen(false)} aria-label="Close chat">
              <CloseIcon fontSize="small" />
            </IconButton>
          </Stack>

          <Box sx={{ px: 1.5, py: 0.75 }}>
            <Typography variant="caption" color="text.secondary">
              Shared admin history (max {maxLines}). Answers use docs + live config only.
            </Typography>
          </Box>
          <Divider />

          <Box sx={{ flex: 1, overflow: 'auto', px: 1.5, py: 1.5 }}>
            {messages.length === 0 && !loading ? (
              <Typography variant="body2" color="text.secondary">
                Ask about SuperFetch, fairness, retries, deployment, or config knobs. Configure an OpenRouter key
                under{' '}
                <Link component={RouterLink} to="/ai" onClick={() => setOpen(false)}>
                  AI Settings
                </Link>
                .
              </Typography>
            ) : null}
            <Stack spacing={1.25}>
              {messages.map((m, idx) => (
                <Box
                  key={m.id || `${m.role}-${idx}-${m.at}`}
                  sx={{
                    alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start',
                    maxWidth: '92%',
                    bgcolor: m.role === 'user' ? 'primary.main' : 'action.hover',
                    color: m.role === 'user' ? 'primary.contrastText' : 'text.primary',
                    px: 1.25,
                    py: 1,
                    borderRadius: 1.5,
                  }}
                >
                  <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                    {m.content}
                  </Typography>
                </Box>
              ))}
              {loading ? (
                <Stack direction="row" spacing={1} alignItems="center">
                  <CircularProgress size={16} />
                  <Typography variant="caption" color="text.secondary">
                    Thinking…
                  </Typography>
                </Stack>
              ) : null}
              <div ref={bottomRef} />
            </Stack>
          </Box>

          {error ? (
            <Typography variant="caption" color="error" sx={{ px: 1.5, pb: 0.5 }}>
              {error}
            </Typography>
          ) : null}

          <Stack direction="row" spacing={1} sx={{ p: 1.25, borderTop: 1, borderColor: 'divider' }}>
            <TextField
              size="small"
              fullWidth
              placeholder="Ask about kafka-batch…"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault()
                  void send()
                }
              }}
              disabled={loading}
              multiline
              maxRows={3}
            />
            <Button variant="contained" onClick={() => void send()} disabled={loading || !input.trim()} sx={{ minWidth: 44 }}>
              <SendIcon fontSize="small" />
            </Button>
          </Stack>
        </Paper>
      ) : null}
    </>
  )
}
