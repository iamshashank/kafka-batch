import { useCallback, useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Typography from '@mui/material/Typography'
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { MultiLineChart, Sparkline } from '../components/MultiLineChart'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

type PerfPoint = {
  t: number
  processed: number
  failed: number
  retried: number
  reclaimed: number
  rtt_avg_ms?: number
  rtt_max_ms?: number
  rtt_errors?: number
}
type PerfJobType = { job_type: string; processed: number; failed: number; retried: number; sparkline: number[] }
type PerfData = {
  ok: boolean
  enabled: boolean
  available: boolean
  message?: string
  range: string
  bucket_seconds: number | null
  points: PerfPoint[]
  job_types: PerfJobType[]
  totals: { processed?: number; failed?: number; retried?: number; reclaimed?: number }
  rtt?: { avg_ms?: number; max_ms?: number; errors?: number; latest_avg_ms?: number; latest_max_ms?: number }
}

const RANGES: { value: string; label: string }[] = [
  { value: '5m', label: '5 min' },
  { value: '1h', label: '1 hour' },
  { value: '3h', label: '3 hours' },
  { value: '24h', label: '24 hours' },
]

const COLORS = {
  processed: '#0f766e',
  failed: '#b91c1c',
  retried: '#b45309',
  reclaimed: '#4338ca',
  rttAvg: '#0369a1',
  rttMax: '#7c3aed',
  rttErrors: '#be123c',
}

function fmtTime(epochSeconds: number): string {
  const d = new Date(epochSeconds * 1000)
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function pct(numerator: number, denominator: number): string {
  if (denominator <= 0) return '—'
  return `${((numerator / denominator) * 100).toFixed(1)}%`
}

export function PerformancePage() {
  const [params, setParams] = useSearchParams()
  const range = params.get('range') || '1h'
  const [data, setData] = useState<PerfData | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      setData(await apiGet<PerfData>(`/api/performance?range=${encodeURIComponent(range)}`))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [range])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>
  if (!data) return null

  const rangeChips = (
    <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
      {RANGES.map((r) => (
        <Chip
          key={r.value}
          label={r.label}
          clickable
          color={range === r.value ? 'primary' : 'default'}
          variant={range === r.value ? 'filled' : 'outlined'}
          onClick={() => setParams(r.value === '1h' ? {} : { range: r.value })}
        />
      ))}
    </Stack>
  )

  return (
    <Box>
      <PageHeader
        title="Performance"
        subtitle="Redis-backed throughput history from job.processed / job.retried / job.failed, reclaim sweeps, and cluster-wide Redis RTT probes."
        actions={rangeChips}
      />

      {!data.enabled ? (
        <EmptyState
          title="Performance metrics"
          message={data.message || 'Enable with config.performance_metrics_enabled = true.'}
        />
      ) : !data.available ? (
        <Alert severity="warning">{data.message || 'Redis is not currently reachable.'}</Alert>
      ) : (
        <PerformanceBody data={data} />
      )}
    </Box>
  )
}

function PerformanceBody({ data }: { data: PerfData }) {
  const processed = data.totals.processed || 0
  const failed = data.totals.failed || 0
  const retried = data.totals.retried || 0
  const reclaimed = data.totals.reclaimed || 0
  const rtt = data.rtt || {}
  const fmtMs = (v: number | undefined) => {
    const n = v || 0
    if (n <= 0) return '—'
    if (n >= 100) return `${Math.round(n)} ms`
    if (n >= 10) return `${n.toFixed(1)} ms`
    return `${n.toFixed(2)} ms`
  }

  const points = data.points || []
  const throughputSeries = [
    { key: 'processed', label: 'Processed', color: COLORS.processed, values: points.map((p) => p.processed) },
    { key: 'failed', label: 'Failed', color: COLORS.failed, values: points.map((p) => p.failed) },
    { key: 'retried', label: 'Retried', color: COLORS.retried, values: points.map((p) => p.retried) },
  ]
  const reclaimSeries = [
    { key: 'reclaimed', label: 'Reclaimed (orphaned jobs)', color: COLORS.reclaimed, values: points.map((p) => p.reclaimed) },
  ]
  const rttSeries = [
    { key: 'rtt_avg', label: 'Avg RTT', color: COLORS.rttAvg, values: points.map((p) => p.rtt_avg_ms || 0) },
    { key: 'rtt_max', label: 'Max RTT', color: COLORS.rttMax, values: points.map((p) => p.rtt_max_ms || 0) },
  ]
  const rttErrorSeries = [
    { key: 'rtt_errors', label: 'Probe errors', color: COLORS.rttErrors, values: points.map((p) => p.rtt_errors || 0) },
  ]

  const rangeCaption =
    points.length > 0
      ? `${fmtTime(points[0].t)} – ${fmtTime(points[points.length - 1].t)} · ${points.length} points · ~${data.bucket_seconds}s/bucket`
      : null

  return (
    <>
      <MetricCards
        metrics={[
          { label: 'Processed', value: processed, color: COLORS.processed },
          { label: 'Failed', value: failed, color: COLORS.failed },
          { label: 'Retried', value: retried, color: COLORS.retried },
          { label: 'Reclaimed', value: reclaimed, color: COLORS.reclaimed },
          { label: 'Success rate', value: pct(processed, processed + failed) },
          { label: 'Redis RTT (latest)', value: fmtMs(rtt.latest_avg_ms) },
          { label: 'Redis RTT max (range)', value: fmtMs(rtt.max_ms) },
          { label: 'RTT probe errors', value: rtt.errors || 0, color: COLORS.rttErrors },
        ]}
      />

      <SectionCard title="System throughput" subheader={rangeCaption || undefined}>
        <MultiLineChart
          series={throughputSeries}
          timestamps={points.map((p) => p.t)}
          bucketSeconds={data.bucket_seconds}
          emptyMessage="No processed/failed/retried events recorded in this range yet."
        />
      </SectionCard>

      <SectionCard
        title="Redis RTT"
        subheader="Cluster-wide PING probes (~one winner every 15s). Avg and max per bucket."
      >
        <MultiLineChart
          series={rttSeries}
          timestamps={points.map((p) => p.t)}
          valueUnit="ms"
          emptyMessage="No Redis RTT probes recorded in this range yet."
        />
      </SectionCard>

      <SectionCard title="Redis probe errors" subheader="Timed-out or failed PING probes in each bucket.">
        <MultiLineChart
          series={rttErrorSeries}
          timestamps={points.map((p) => p.t)}
          height={100}
          valueUnit="errors"
          emptyMessage="No probe errors in this range."
        />
      </SectionCard>

      <SectionCard title="Reclaim rate" subheader="Orphaned SuperFetch jobs re-produced by the reclaim scheduler.">
        <MultiLineChart
          series={reclaimSeries}
          timestamps={points.map((p) => p.t)}
          bucketSeconds={data.bucket_seconds}
          height={100}
          emptyMessage="No reclaim sweeps found any orphans in this range."
        />
      </SectionCard>

      <SectionCard title="Top job types" noPadding>
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Job type</TableCell>
                <TableCell align="right">Processed</TableCell>
                <TableCell align="right">Failed</TableCell>
                <TableCell align="right">Retried</TableCell>
                <TableCell align="right">Success</TableCell>
                <TableCell align="right">Trend</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {data.job_types.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} align="center" sx={{ py: 5 }}>
                    <Typography color="text.secondary">No job activity recorded in this range yet.</Typography>
                  </TableCell>
                </TableRow>
              ) : (
                data.job_types.map((jt) => (
                  <TableRow key={jt.job_type} hover>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{jt.job_type}</TableCell>
                    <TableCell align="right">{jt.processed}</TableCell>
                    <TableCell align="right">{jt.failed}</TableCell>
                    <TableCell align="right">{jt.retried}</TableCell>
                    <TableCell align="right">{pct(jt.processed, jt.processed + jt.failed)}</TableCell>
                    <TableCell align="right">
                      <Box sx={{ display: 'inline-block' }}>
                        <Sparkline values={jt.sparkline} color={COLORS.processed} />
                      </Box>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </SectionCard>
    </>
  )
}
