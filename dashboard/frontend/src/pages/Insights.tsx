import { useState, useRef, useEffect } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import axios from 'axios'
import { Send, BrainCircuit, BarChart2, Table2 } from 'lucide-react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'

type QueryType = 'text2sql' | 'insight' | 'anomaly'

interface ResultRow { [key: string]: unknown }
interface InsightResult {
  insight_id: string
  query_type: string
  user_query: string
  generated_sql?: string
  response: string
  model_used: string
  latency_ms: number
  rows?: ResultRow[]
  columns?: string[]
  exec_error?: string
}

const EXAMPLE_QUERIES = [
  'Show total revenue by region for the last 7 days',
  'Top 5 products by total sales amount',
  'Which IoT locations had temperature anomalies today?',
  'What is the trading volume by asset this week?',
  'Most engaging platform for social posts yesterday?',
]

function ResultTable({ columns, rows }: { columns: string[]; rows: ResultRow[] }) {
  return (
    <div className="overflow-x-auto rounded border border-gray-700">
      <table className="text-xs w-full">
        <thead className="bg-gray-800">
          <tr>
            {columns.map(c => (
              <th key={c} className="px-3 py-2 text-left text-gray-400 font-medium">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 50).map((row, i) => (
            <tr key={i} className="border-t border-gray-800 hover:bg-gray-800/50">
              {columns.map(c => (
                <td key={c} className="px-3 py-1.5 text-gray-300">{String(row[c] ?? '')}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 50 && (
        <p className="text-xs text-gray-500 p-2">Showing 50 of {rows.length} rows</p>
      )}
    </div>
  )
}

function AutoChart({ columns, rows }: { columns: string[]; rows: ResultRow[] }) {
  // Try to render a bar chart if there's one numeric and one string column
  const numCol = columns.find(c => typeof rows[0]?.[c] === 'number' || !isNaN(Number(rows[0]?.[c])))
  const strCol = columns.find(c => c !== numCol)
  if (!numCol || !strCol || rows.length === 0) return null

  const data = rows.slice(0, 20).map(r => ({ name: String(r[strCol]).slice(0, 15), value: Number(r[numCol]) }))
  return (
    <div className="bg-gray-800 rounded-lg p-3 mt-3" style={{ height: 180 }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <XAxis dataKey="name" tick={{ fill: '#9ca3af', fontSize: 10 }} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 10 }} />
          <Tooltip contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }} />
          <Bar dataKey="value" fill="#3b82f6" radius={[3, 3, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

export default function Insights() {
  const [query, setQuery] = useState('')
  const [queryType, setQueryType] = useState<QueryType>('text2sql')
  const [jobId, setJobId] = useState<string | null>(null)
  const [history, setHistory] = useState<InsightResult[]>([])
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const submit = useMutation({
    mutationFn: (q: string) =>
      axios.post('/api/insights/query', { query: q, query_type: queryType }).then(r => r.data),
    onSuccess: (data) => setJobId(data.job_id),
  })

  // Poll for result
  const { data: jobResult } = useQuery({
    queryKey: ['insight-result', jobId],
    queryFn: () => axios.get(`/api/insights/result/${jobId}`).then(r => r.data),
    enabled: !!jobId,
    refetchInterval: (q) => {
      return q.state.data?.status === 'done' ? false : 2000
    },
  })

  useEffect(() => {
    if (jobResult?.status === 'done' && jobResult.result) {
      setHistory(prev => [jobResult.result, ...prev.slice(0, 9)])
      setJobId(null)
    }
  }, [jobResult])

  const handleSubmit = () => {
    if (!query.trim()) return
    submit.mutate(query)
  }

  const isLoading = !!jobId && jobResult?.status !== 'done'
  const latestResult: InsightResult | null = history[0] ?? null

  return (
    <div className="max-w-4xl space-y-5">
      <div>
        <h2 className="text-sm font-semibold text-gray-400 mb-1 uppercase tracking-widest flex items-center gap-2">
          <BrainCircuit size={14} /> AI Query Mode
        </h2>
        <p className="text-xs text-gray-500">
          Ask a question in plain English — the LLM converts it to ClickHouse SQL and runs it.
          Falls back to local Ollama → rules engine if cloud is unavailable.
        </p>
      </div>

      {/* Query type toggle */}
      <div className="flex gap-2 text-xs">
        {(['text2sql', 'insight', 'anomaly'] as QueryType[]).map(t => (
          <button
            key={t}
            onClick={() => setQueryType(t)}
            className={`px-3 py-1.5 rounded transition-colors ${
              queryType === t ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:bg-gray-700'
            }`}
          >
            {t === 'text2sql' ? '🔍 SQL Query' : t === 'insight' ? '💡 Insight' : '⚠️ Anomaly'}
          </button>
        ))}
      </div>

      {/* Example queries */}
      <div className="flex flex-wrap gap-2">
        {EXAMPLE_QUERIES.map(eq => (
          <button
            key={eq}
            onClick={() => setQuery(eq)}
            className="text-xs px-2 py-1 bg-gray-800 hover:bg-gray-700 text-gray-400 rounded transition-colors"
          >
            {eq}
          </button>
        ))}
      </div>

      {/* Input */}
      <div className="flex gap-2">
        <textarea
          ref={textareaRef}
          value={query}
          onChange={e => setQuery(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) handleSubmit() }}
          placeholder="Ask anything about your data…"
          rows={2}
          className="flex-1 bg-gray-800 border border-gray-700 rounded-lg px-4 py-3 text-sm text-gray-200
                     placeholder-gray-500 focus:outline-none focus:border-blue-500 resize-none"
        />
        <button
          onClick={handleSubmit}
          disabled={isLoading || !query.trim()}
          className="px-4 bg-blue-600 hover:bg-blue-500 disabled:opacity-40 disabled:cursor-not-allowed
                     rounded-lg transition-colors flex items-center gap-2 text-sm"
        >
          {isLoading ? (
            <span className="animate-spin">⟳</span>
          ) : (
            <Send size={15} />
          )}
        </button>
      </div>

      {/* Latest result */}
      {isLoading && (
        <div className="text-sm text-gray-400 flex items-center gap-2">
          <span className="animate-pulse">⟳</span> Running query (model: Ollama/Groq)…
        </div>
      )}

      {latestResult && (
        <div className="bg-gray-800 rounded-lg p-4 space-y-3 border border-gray-700">
          <div className="flex items-center justify-between text-xs text-gray-500">
            <span className="flex items-center gap-1">
              <BrainCircuit size={12} /> {latestResult.model_used} · {latestResult.latency_ms}ms
            </span>
            <span>{latestResult.query_type}</span>
          </div>

          <p className="text-sm text-gray-200">{latestResult.response}</p>

          {latestResult.generated_sql && (
            <pre className="bg-gray-900 rounded p-3 text-xs text-green-300 overflow-x-auto">
              {latestResult.generated_sql}
            </pre>
          )}

          {latestResult.exec_error && (
            <p className="text-xs text-red-400">SQL error: {latestResult.exec_error}</p>
          )}

          {latestResult.rows && latestResult.columns && latestResult.rows.length > 0 && (
            <div className="space-y-2">
              <div className="flex items-center gap-2 text-xs text-gray-400">
                <Table2 size={12} /> {latestResult.rows.length} rows
                <BarChart2 size={12} className="ml-2" /> auto-chart
              </div>
              <AutoChart columns={latestResult.columns} rows={latestResult.rows} />
              <ResultTable columns={latestResult.columns} rows={latestResult.rows} />
            </div>
          )}
        </div>
      )}

      {/* History */}
      {history.length > 1 && (
        <div>
          <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-widest mb-2">Query history</h3>
          <div className="space-y-2">
            {history.slice(1).map(r => (
              <div key={r.insight_id}
                   className="bg-gray-900 rounded p-3 text-xs cursor-pointer hover:bg-gray-800 transition-colors"
                   onClick={() => setQuery(r.user_query)}>
                <span className="text-gray-400">{r.user_query}</span>
                <span className="ml-2 text-gray-600">· {r.model_used}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
