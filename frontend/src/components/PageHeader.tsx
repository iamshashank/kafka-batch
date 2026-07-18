import Box from '@mui/material/Box'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import type { ReactNode } from 'react'

export function PageHeader({
  title,
  subtitle,
  actions,
}: {
  title: string
  subtitle?: string
  actions?: ReactNode
}) {
  return (
    <Stack
      direction="row"
      spacing={1.5}
      alignItems="flex-start"
      justifyContent="space-between"
      sx={{ mb: 2.5, width: '100%' }}
    >
      <Box sx={{ minWidth: 0, flex: '1 1 auto', pr: actions ? 2 : 0 }}>
        <Typography variant="h5" component="h1" sx={{ mb: 0.5 }}>
          {title}
        </Typography>
        {subtitle ? (
          <Typography variant="body2" color="text.secondary">
            {subtitle}
          </Typography>
        ) : null}
      </Box>
      {actions ? (
        <Box sx={{ flex: '0 0 auto', ml: 'auto', alignSelf: 'center' }}>{actions}</Box>
      ) : null}
    </Stack>
  )
}
