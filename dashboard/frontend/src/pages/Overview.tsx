import { useQuery } from '@tanstack/react-query'
import axios from 'axios'
import ReactFlow, { Background, Controls, MiniMap, Node, Edge } from 'reactflow'
import 'reactflow/dist/style.css'

const STATUS_COLORS: Record<string, string> = {
  healthy: 'bg-green-500',
  degraded: 'bg-yellow-500',
  unreachable: 'bg-red-500',
}

function ServiceBadge({ name, status, port }: { name: string; status: string; port: number }) {
  return (
    <div className="flex items-center gap-2 bg-gray-800 rounded px-3 py-2 text-xs">
      <span className={`w-2 h-2 rounded-full ${STATUS_COLORS[status] ?? 'bg-gray-500'}`} />
      <span className="text-gray-200">{name}</span>
      <span className="text-gray-500">:{port}</span>
    </div>
  )
}

// Map API topology nodes → ReactFlow nodes
function toRFNodes(nodes: any[]): Node[] {
  const typeColors: Record<string, string> = {
    source: '#1d4ed8', stream: '#7c3aed', consumer: '#0891b2',
    storage: '#065f46', processing: '#92400e', warehouse: '#1e3a5f',
    ai: '#4c1d95', output: '#374151',
  }
  return nodes.map(n => ({
    id: n.id,
    position: { x: n.x, y: n.y },
    data: { label: n.label },
    style: {
      background: typeColors[n.type] ?? '#374151',
      color: '#f9fafb',
      border: '1px solid #4b5563',
      borderRadius: 8,
      padding: '6px 12px',
      fontSize: 12,
    },
  }))
}

function toRFEdges(edges: any[]): Edge[] {
  return edges.map((e, i) => ({
    id: `e${i}`,
    source: e.source,
    target: e.target,
    label: e.label,
    animated: true,
    style: { stroke: '#4b5563' },
    labelStyle: { fill: '#9ca3af', fontSize: 10 },
  }))
}

export default function Overview() {
  const { data: health } = useQuery({
    queryKey: ['pipeline-health'],
    queryFn: () => axios.get('/api/pipeline/health').then(r => r.data),
    refetchInterval: 10000,
  })

  const { data: topo } = useQuery({
    queryKey: ['pipeline-topology'],
    queryFn: () => axios.get('/api/pipeline/topology').then(r => r.data),
  })

  const rfNodes = topo ? toRFNodes(topo.nodes) : []
  const rfEdges = topo ? toRFEdges(topo.edges) : []

  return (
    <div className="space-y-6">
      {/* Service status badges */}
      {health?.services && (
        <div>
          <h2 className="text-sm font-semibold text-gray-400 mb-3 uppercase tracking-widest">Services</h2>
          <div className="flex flex-wrap gap-2">
            {health.services.map((s: any) => (
              <ServiceBadge key={s.name} {...s} />
            ))}
          </div>
        </div>
      )}

      {/* Pipeline topology graph */}
      <div>
        <h2 className="text-sm font-semibold text-gray-400 mb-3 uppercase tracking-widest">Pipeline Topology</h2>
        <div className="rounded-lg border border-gray-700 overflow-hidden" style={{ height: 420 }}>
          <ReactFlow nodes={rfNodes} edges={rfEdges} fitView>
            <Background color="#374151" gap={20} />
            <Controls />
            <MiniMap nodeColor={() => '#4b5563'} maskColor="rgba(0,0,0,0.6)" />
          </ReactFlow>
        </div>
      </div>
    </div>
  )
}
