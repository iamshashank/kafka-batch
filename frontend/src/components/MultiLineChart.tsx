import { useCallback, useState, type MouseEvent } from 'react'
import Box from '@mui/material/Box'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'

export type ChartSeries = {
  key: string
  label: string
  color: string
  values: number[]
}

type HoverState = {
  index: number
  clientX: number
  clientY: number
}

const VIEW_WIDTH = 300

function fmtRate(count: number, bucketSeconds: number | null | undefined): string | null {
  if (!bucketSeconds || bucketSeconds <= 0) return null
  const perMin = (count / bucketSeconds) * 60
  if (perMin >= 100) return `${Math.round(perMin)}/min`
  if (perMin >= 10) return `${perMin.toFixed(1)}/min`
  return `${perMin.toFixed(2)}/min`
}

function fmtHoverTime(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

/** Minimal dependency-free SVG multi-line chart (no chart lib required). */
export function MultiLineChart({
  series,
  timestamps,
  bucketSeconds,
  height = 160,
  emptyMessage = 'No data yet.',
  valueUnit = 'jobs',
}: {
  series: ChartSeries[]
  /** Epoch seconds aligned with each point index (enables hover tooltip). */
  timestamps?: number[]
  /** Bucket width in seconds; used to show jobs/min on hover. */
  bucketSeconds?: number | null
  height?: number
  emptyMessage?: string
  valueUnit?: string
}) {
  const [hover, setHover] = useState<HoverState | null>(null)
  const allValues = series.flatMap((s) => s.values)
  const pointCount = Math.max(0, ...series.map((s) => s.values.length))
  const hasData = pointCount > 0 && allValues.some((v) => v > 0)

  const onMove = useCallback(
    (e: MouseEvent<SVGSVGElement>) => {
      if (pointCount <= 0) return
      const rect = e.currentTarget.getBoundingClientRect()
      const xRatio = rect.width > 0 ? (e.clientX - rect.left) / rect.width : 0
      const index = Math.max(0, Math.min(pointCount - 1, Math.round(xRatio * (pointCount - 1 || 1))))
      setHover({ index, clientX: e.clientX, clientY: e.clientY })
    },
    [pointCount],
  )

  const onLeave = useCallback(() => setHover(null), [])

  if (!hasData) {
    return (
      <Box sx={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Typography variant="body2" color="text.secondary">
          {emptyMessage}
        </Typography>
      </Box>
    )
  }

  const maxValue = Math.max(1, ...allValues)
  const stepX = pointCount > 1 ? VIEW_WIDTH / (pointCount - 1) : 0
  const pad = 4

  const pathFor = (values: number[]) =>
    values
      .map((v, i) => {
        const x = i * stepX
        const y = height - pad - (v / maxValue) * (height - pad * 2)
        return `${i === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`
      })
      .join(' ')

  const hoverIndex = hover?.index ?? null
  const hoverX = hoverIndex != null ? hoverIndex * stepX : null
  const tipTime =
    hoverIndex != null && timestamps?.[hoverIndex] != null ? fmtHoverTime(timestamps[hoverIndex]) : null

  return (
    <Box sx={{ position: 'relative' }}>
      <Box
        component="svg"
        viewBox={`0 0 ${VIEW_WIDTH} ${height}`}
        preserveAspectRatio="none"
        onMouseMove={onMove}
        onMouseLeave={onLeave}
        sx={{ width: '100%', height, display: 'block', color: 'divider', cursor: 'crosshair' }}
      >
        {[0.25, 0.5, 0.75].map((f) => (
          <line key={f} x1={0} x2={VIEW_WIDTH} y1={height * f} y2={height * f} stroke="currentColor" strokeWidth={1} />
        ))}
        {series.map((s) => (
          <path key={s.key} d={pathFor(s.values)} fill="none" stroke={s.color} strokeWidth={1.75} vectorEffect="non-scaling-stroke" />
        ))}
        {hoverX != null && (
          <line
            x1={hoverX}
            x2={hoverX}
            y1={0}
            y2={height}
            stroke="currentColor"
            strokeWidth={1}
            strokeDasharray="3 3"
            opacity={0.7}
          />
        )}
        {hoverIndex != null &&
          series.map((s) => {
            const v = s.values[hoverIndex] ?? 0
            const y = height - pad - (v / maxValue) * (height - pad * 2)
            return <circle key={s.key} cx={hoverX!} cy={y} r={3.5} fill={s.color} stroke="#fff" strokeWidth={1.25} />
          })}
      </Box>

      {hover && hoverIndex != null && (
        <Paper
          elevation={4}
          sx={{
            position: 'fixed',
            left: Math.min(hover.clientX + 12, typeof window !== 'undefined' ? window.innerWidth - 220 : hover.clientX + 12),
            top: Math.min(hover.clientY + 12, typeof window !== 'undefined' ? window.innerHeight - 140 : hover.clientY + 12),
            zIndex: 1300,
            px: 1.25,
            py: 1,
            pointerEvents: 'none',
            minWidth: 160,
          }}
        >
          {tipTime && (
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
              {tipTime}
              {bucketSeconds ? ` · ${bucketSeconds}s bucket` : null}
            </Typography>
          )}
          <Stack spacing={0.35}>
            {series.map((s) => {
              const count = s.values[hoverIndex] ?? 0
              const rate = fmtRate(count, bucketSeconds)
              return (
                <Stack key={s.key} direction="row" spacing={1} alignItems="baseline" justifyContent="space-between">
                  <Stack direction="row" spacing={0.75} alignItems="center">
                    <Box sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: s.color, flexShrink: 0 }} />
                    <Typography variant="caption">{s.label}</Typography>
                  </Stack>
                  <Typography variant="caption" sx={{ fontVariantNumeric: 'tabular-nums', fontWeight: 600 }}>
                    {count} {valueUnit}
                    {rate ? ` · ${rate}` : null}
                  </Typography>
                </Stack>
              )
            })}
          </Stack>
        </Paper>
      )}

      <Stack direction="row" spacing={2} flexWrap="wrap" useFlexGap sx={{ mt: 1 }}>
        {series.map((s) => (
          <Stack key={s.key} direction="row" spacing={0.75} alignItems="center">
            <Box sx={{ width: 9, height: 9, borderRadius: '50%', bgcolor: s.color, flexShrink: 0 }} />
            <Typography variant="caption" color="text.secondary">
              {s.label}
            </Typography>
          </Stack>
        ))}
      </Stack>
    </Box>
  )
}

/** Compact single-series sparkline (per-job-type table rows). */
export function Sparkline({
  values,
  color = '#0f766e',
  width = 120,
  height = 28,
}: {
  values: number[]
  color?: string
  width?: number
  height?: number
}) {
  const hasData = values.length > 0 && values.some((v) => v > 0)
  if (!hasData) {
    return (
      <Box sx={{ width, height, display: 'flex', alignItems: 'center' }}>
        <Box sx={{ width: '100%', height: 1, bgcolor: 'divider' }} />
      </Box>
    )
  }

  const max = Math.max(1, ...values)
  const stepX = values.length > 1 ? width / (values.length - 1) : 0
  const pad = 2
  const d = values
    .map((v, i) => {
      const x = i * stepX
      const y = height - pad - (v / max) * (height - pad * 2)
      return `${i === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`
    })
    .join(' ')

  return (
    <Box component="svg" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" sx={{ width, height, display: 'block' }}>
      <path d={d} fill="none" stroke={color} strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
    </Box>
  )
}
