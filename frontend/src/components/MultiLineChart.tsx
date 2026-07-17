import Box from '@mui/material/Box'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'

export type ChartSeries = {
  key: string
  label: string
  color: string
  values: number[]
}

const VIEW_WIDTH = 300

/** Minimal dependency-free SVG multi-line chart (no chart lib required). */
export function MultiLineChart({
  series,
  height = 160,
  emptyMessage = 'No data yet.',
}: {
  series: ChartSeries[]
  height?: number
  emptyMessage?: string
}) {
  const allValues = series.flatMap((s) => s.values)
  const pointCount = Math.max(0, ...series.map((s) => s.values.length))
  const hasData = pointCount > 0 && allValues.some((v) => v > 0)

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

  return (
    <Box>
      <Box
        component="svg"
        viewBox={`0 0 ${VIEW_WIDTH} ${height}`}
        preserveAspectRatio="none"
        sx={{ width: '100%', height, display: 'block', color: 'divider' }}
      >
        {[0.25, 0.5, 0.75].map((f) => (
          <line key={f} x1={0} x2={VIEW_WIDTH} y1={height * f} y2={height * f} stroke="currentColor" strokeWidth={1} />
        ))}
        {series.map((s) => (
          <path key={s.key} d={pathFor(s.values)} fill="none" stroke={s.color} strokeWidth={1.75} vectorEffect="non-scaling-stroke" />
        ))}
      </Box>
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
