import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import axios from 'axios'
import { Play, Square, Zap } from 'lucide-react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'

const TOPIC_COLORS: Record<string, string> = {
  transactions: '#3b82f6',
  sensors: '#10b981',
  financial: '#f59e0b',
  social: '#8b5cf6',
}

export default function Simulation() {
  const qc = useQueryClient()
  const [rateInput, setRateInput] = useState<number>(50)

  const { data: status } = useQuery({
    queryKey: ['sim-status'],
    queryFn: () => axios.get('/api/simulation/status').then(r => r.data),
    refetchInterval: 3000,
  })

  const { data: counters } = useQuery({
    queryKey: ['sim-counters'],
    queryFn: () => axios.get('/api/simulation/counters').then(r => r.data),
    refetchInterval: 3000,
  })

  const setRate = useMutation({
    mutationFn: (rate: number) =>
      axios.post('/api/simulation/rate', { events_per_second: rate }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['sim-status'] }),
  })

  const start = useMutation({
    mutationFn: () => axios.post('/api/simulation/start'),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['sim-status'] }),
  })

  const stop = useMutation({
    mutationFn: () => axios.post('/api/simulation/stop'),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['sim-status'] }),
  })

  const chartData = counters
    ? Object.entries(counters).map(([topic, count]) => ({ topic, count }))
    : []

  return (
    <div className="space-y-6 max-w-3xl">
      <div>
        <h2 className="text-sm font-semibold text-gray-400 mb-4 uppercase tracking-widest">Simulation Control</h2>

        {/* Status + controls */}
        <div className="bg-gray-800 rounded-lg p-5 space-y-4">
          <div className="flex items-center gap-4">
            <span className="text-sm text-gray-300">Current rate:</span>
            <span className="text-2xl font-bold text-blue-400">
              {status?.events_per_second ?? '—'} <span className="text-sm font-normal text-gray-400">events/sec</span>
            </span>
            <span className={`ml-auto px-2 py-1 rounded text-xs font-medium ${
              (status?.events_per_second ?? 0) > 0 ? 'bg-green-900 text-green-300' : 'bg-gray-700 text-gray-400'
            }`}>
              {(status?.events_per_second ?? 0) > 0 ? 'RUNNING' : 'STOPPED'}
            </span>
          </div>

          {/* Rate slider */}
          <div className="space-y-2">
            <label className="text-xs text-gray-400">Target rate: {rateInput} events/sec</label>
            <input
              type="range" min={0} max={2000} step={10}
              value={rateInput}
              onChange={e => setRateInput(Number(e.target.value))}
              className="w-full accent-blue-500"
            />
            <div className="flex justify-between text-xs text-gray-500">
              <span>0</span><span>500</span><span>1000</span><span>1500</span><span>2000</span>
            </div>
          </div>

          {/* Buttons */}
          <div className="flex gap-3">
            <button
              onClick={() => setRate.mutate(rateInput)}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 rounded text-sm transition-colors"
            >
              <Zap size={14} /> Apply rate
            </button>
            <button
              onClick={() => start.mutate()}
              className="flex items-center gap-2 px-4 py-2 bg-green-700 hover:bg-green-600 rounded text-sm transition-colors"
            >
              <Play size={14} /> Start
            </button>
            <button
              onClick={() => stop.mutate()}
              className="flex items-center gap-2 px-4 py-2 bg-red-800 hover:bg-red-700 rounded text-sm transition-colors"
            >
              <Square size={14} /> Stop
            </button>
          </div>
        </div>
      </div>

      {/* Event counters */}
      {chartData.length > 0 && (
        <div>
          <h2 className="text-sm font-semibold text-gray-400 mb-3 uppercase tracking-widest">Events produced (total)</h2>
          <div className="bg-gray-800 rounded-lg p-4" style={{ height: 200 }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData}>
                <XAxis dataKey="topic" tick={{ fill: '#9ca3af', fontSize: 12 }} />
                <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
                <Tooltip
                  contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                  labelStyle={{ color: '#f3f4f6' }}
                />
                <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                  {chartData.map(entry => (
                    <Cell key={entry.topic} fill={TOPIC_COLORS[entry.topic] ?? '#6b7280'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </div>
  )
}
