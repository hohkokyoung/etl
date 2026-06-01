import { useQuery } from '@tanstack/react-query'
import axios from 'axios'
import { Database, HardDrive, FileStack } from 'lucide-react'

interface SourceStats { files: number; size_bytes: number }
interface LayerStats {
  total_files: number
  total_size_mb: number
  by_source: Record<string, SourceStats>
  error?: string
}

const LAYER_CONFIG = {
  bronze: { label: 'Bronze (Raw)',    color: 'border-amber-700  text-amber-400',  icon: '🥉' },
  silver: { label: 'Silver (Typed)',  color: 'border-slate-400  text-slate-300',  icon: '🥈' },
  gold:   { label: 'Gold (Curated)',  color: 'border-yellow-600 text-yellow-400', icon: '🥇' },
}

function LayerCard({ layer, stats }: { layer: keyof typeof LAYER_CONFIG; stats: LayerStats }) {
  const cfg = LAYER_CONFIG[layer]
  return (
    <div className={`bg-gray-800 rounded-lg p-4 border ${cfg.color.split(' ')[0]}`}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <span className="text-lg">{cfg.icon}</span>
          <span className={`font-semibold text-sm ${cfg.color.split(' ')[1]}`}>{cfg.label}</span>
        </div>
        <div className="text-right text-xs text-gray-400">
          <div className="flex items-center gap-1 justify-end">
            <FileStack size={11} /> {stats.total_files} files
          </div>
          <div className="flex items-center gap-1 justify-end">
            <HardDrive size={11} /> {stats.total_size_mb} MB
          </div>
        </div>
      </div>
      {stats.error ? (
        <p className="text-xs text-red-400">{stats.error}</p>
      ) : (
        <div className="space-y-1">
          {Object.entries(stats.by_source ?? {}).map(([src, s]) => (
            <div key={src} className="flex items-center justify-between text-xs">
              <span className="text-gray-300">{src}</span>
              <span className="text-gray-500">{s.files} files · {(s.size_bytes / 1024).toFixed(1)} KB</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function LakeExplorer() {
  const { data, isLoading } = useQuery({
    queryKey: ['lake-stats'],
    queryFn: () => axios.get('/api/lake/stats').then(r => r.data),
    refetchInterval: 30000,
  })

  const { data: buckets } = useQuery({
    queryKey: ['lake-buckets'],
    queryFn: () => axios.get('/api/lake/buckets').then(r => r.data),
  })

  return (
    <div className="max-w-3xl space-y-5">
      <div className="flex items-center gap-2">
        <Database size={16} className="text-blue-400" />
        <h2 className="text-sm font-semibold text-gray-400 uppercase tracking-widest">Data Lake — Floci S3</h2>
      </div>

      {buckets && (
        <div className="flex flex-wrap gap-2">
          {buckets.buckets?.map((b: string) => (
            <span key={b} className="text-xs bg-gray-800 px-2 py-1 rounded text-gray-400">
              s3://{b}
            </span>
          ))}
        </div>
      )}

      {isLoading ? (
        <p className="text-sm text-gray-500 animate-pulse">Loading lake stats…</p>
      ) : data ? (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {(Object.keys(LAYER_CONFIG) as (keyof typeof LAYER_CONFIG)[]).map(layer => (
            data[layer] && <LayerCard key={layer} layer={layer} stats={data[layer]} />
          ))}
        </div>
      ) : null}

      <p className="text-xs text-gray-600">
        Data format: Bronze = raw Parquet · Silver = Iceberg (typed) · Gold = Iceberg (aggregated)
      </p>
    </div>
  )
}
