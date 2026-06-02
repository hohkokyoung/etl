import { useState } from 'react'
import { Activity, Database, FlaskConical, BarChart2, BrainCircuit } from 'lucide-react'
import Overview from './pages/Overview'
import Simulation from './pages/Simulation'
import Insights from './pages/Insights'
import LakeExplorer from './components/LakeExplorer/LakeExplorer'

type Tab = 'overview' | 'simulation' | 'lake' | 'ai-query'

const tabs: { id: Tab; label: string; icon: React.ReactNode }[] = [
  { id: 'ai-query',   label: 'AI Query',    icon: <BrainCircuit size={16} /> },
  { id: 'overview',   label: 'Pipeline',    icon: <Activity size={16} /> },
  { id: 'simulation', label: 'Simulation',  icon: <FlaskConical size={16} /> },
  { id: 'lake',       label: 'Data Lake',   icon: <Database size={16} /> },
]

export default function App() {
  const [activeTab, setActiveTab] = useState<Tab>('ai-query')

  return (
    <div className="min-h-screen flex flex-col">
      {/* Header */}
      <header className="border-b border-gray-800 bg-gray-900 px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <BarChart2 className="text-blue-400" size={22} />
          <span className="text-lg font-semibold tracking-tight">ETL Platform</span>
          <span className="text-xs text-gray-500 ml-2">on-prem · offline</span>
        </div>
        <div className="flex items-center gap-4 text-xs text-gray-400">
          <a href="http://localhost:8082" target="_blank" rel="noreferrer"
             className="hover:text-blue-400 transition-colors">Kafka UI ↗</a>
          <a href="http://localhost:8080" target="_blank" rel="noreferrer"
             className="hover:text-blue-400 transition-colors">Airflow ↗</a>
          <a href="http://localhost:8083" target="_blank" rel="noreferrer"
             className="hover:text-blue-400 transition-colors">Spark ↗</a>
          <a href="http://localhost:3001" target="_blank" rel="noreferrer"
             className="hover:text-blue-400 transition-colors">Grafana ↗</a>
        </div>
      </header>

      {/* Tabs */}
      <nav className="flex border-b border-gray-800 bg-gray-900 px-6">
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-4 py-3 text-sm border-b-2 transition-colors ${
              activeTab === tab.id
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-200'
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </nav>

      {/* Page content */}
      <main className="flex-1 overflow-auto p-6">
        {activeTab === 'overview'   && <Overview />}
        {activeTab === 'simulation' && <Simulation />}
        {activeTab === 'lake'       && <LakeExplorer />}
        {activeTab === 'ai-query'   && <Insights />}
      </main>
    </div>
  )
}
