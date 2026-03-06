import { useState, useEffect, useReducer } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ScoreBucket {
  bucket: string
  count: number
}

export interface TimelineEntry {
  date: string
  submissions: number
}

export interface PassRateEntry {
  task: string
  avg_score: number
  attempts: number
}

export interface LabItem {
  id: number
  type: string
  title: string
  parent_id: number | null
  created_at: string
}

type FetchStatus = 'idle' | 'loading' | 'success' | 'error'

interface DashboardState {
  status: FetchStatus
  errorMessage: string
  labs: LabItem[]
  selectedLab: string
  scores: ScoreBucket[]
  timeline: TimelineEntry[]
  passRates: PassRateEntry[]
}

type DashboardAction =
  | { type: 'set_loading' }
  | { type: 'set_success'; labs: LabItem[] }
  | { type: 'set_error'; message: string }
  | { type: 'set_scores'; scores: ScoreBucket[] }
  | { type: 'set_timeline'; timeline: TimelineEntry[] }
  | { type: 'set_pass_rates'; passRates: PassRateEntry[] }
  | { type: 'set_selected_lab'; lab: string }

// ---------------------------------------------------------------------------
// Reducer
// ---------------------------------------------------------------------------

const initialState: DashboardState = {
  status: 'idle',
  errorMessage: '',
  labs: [],
  selectedLab: '',
  scores: [],
  timeline: [],
  passRates: [],
}

function dashboardReducer(
  state: DashboardState,
  action: DashboardAction,
): DashboardState {
  switch (action.type) {
    case 'set_loading':
      return { ...state, status: 'loading' }
    case 'set_success':
      return {
        ...state,
        status: 'success',
        labs: action.labs,
        selectedLab: action.labs[0]?.title ?? '',
      }
    case 'set_error':
      return { ...state, status: 'error', errorMessage: action.message }
    case 'set_scores':
      return { ...state, scores: action.scores }
    case 'set_timeline':
      return { ...state, timeline: action.timeline }
    case 'set_pass_rates':
      return { ...state, passRates: action.passRates }
    case 'set_selected_lab':
      return { ...state, selectedLab: action.lab }
    default:
      return state
  }
}

// ---------------------------------------------------------------------------
// API Functions
// ---------------------------------------------------------------------------

const STORAGE_KEY = 'api_key'

function getApiKey(): string {
  return localStorage.getItem(STORAGE_KEY) ?? ''
}

async function fetchLabs(token: string): Promise<LabItem[]> {
  const response = await fetch('/items/', {
    headers: { Authorization: `Bearer ${token}` },
  })
  if (!response.ok) {
    throw new Error(`Failed to fetch labs: HTTP ${response.status}`)
  }
  const items: LabItem[] = await response.json()
  return items.filter((item) => item.type === 'lab')
}

async function fetchScores(
  token: string,
  lab: string,
): Promise<ScoreBucket[]> {
  const response = await fetch(`/analytics/scores?lab=${encodeURIComponent(lab)}`, {
    headers: { Authorization: `Bearer ${token}` },
  })
  if (!response.ok) {
    throw new Error(`Failed to fetch scores: HTTP ${response.status}`)
  }
  return response.json()
}

async function fetchTimeline(
  token: string,
  lab: string,
): Promise<TimelineEntry[]> {
  const response = await fetch(`/analytics/timeline?lab=${encodeURIComponent(lab)}`, {
    headers: { Authorization: `Bearer ${token}` },
  })
  if (!response.ok) {
    throw new Error(`Failed to fetch timeline: HTTP ${response.status}`)
  }
  return response.json()
}

async function fetchPassRates(
  token: string,
  lab: string,
): Promise<PassRateEntry[]> {
  const response = await fetch(`/analytics/pass-rates?lab=${encodeURIComponent(lab)}`, {
    headers: { Authorization: `Bearer ${token}` },
  })
  if (!response.ok) {
    throw new Error(`Failed to fetch pass rates: HTTP ${response.status}`)
  }
  return response.json()
}

// ---------------------------------------------------------------------------
// Chart Data Helpers
// ---------------------------------------------------------------------------

function buildScoreChartData(scores: ScoreBucket[]) {
  const labels = scores.map((s) => s.bucket)
  const data = scores.map((s) => s.count)

  return {
    labels,
    datasets: [
      {
        label: 'Number of Submissions',
        data,
        backgroundColor: [
          'rgba(255, 99, 132, 0.6)',
          'rgba(255, 159, 64, 0.6)',
          'rgba(75, 192, 192, 0.6)',
          'rgba(54, 162, 235, 0.6)',
        ],
        borderColor: [
          'rgb(255, 99, 132)',
          'rgb(255, 159, 64)',
          'rgb(75, 192, 192)',
          'rgb(54, 162, 235)',
        ],
        borderWidth: 1,
      },
    ],
  }
}

function buildTimelineData(timeline: TimelineEntry[]) {
  const labels = timeline.map((t) => t.date)
  const data = timeline.map((t) => t.submissions)

  return {
    labels,
    datasets: [
      {
        label: 'Submissions per Day',
        data,
        borderColor: 'rgb(54, 162, 235)',
        backgroundColor: 'rgba(54, 162, 235, 0.2)',
        fill: true,
        tension: 0.1,
      },
    ],
  }
}

const chartOptions = {
  responsive: true,
  plugins: {
    legend: {
      position: 'top' as const,
    },
  },
  scales: {
    y: {
      beginAtZero: true,
      ticks: {
        stepSize: 1,
      },
    },
  },
}

// ---------------------------------------------------------------------------
// Dashboard Component
// ---------------------------------------------------------------------------

export default function Dashboard() {
  const [state, dispatch] = useReducer(dashboardReducer, initialState)
  const [token] = useState<string>(getApiKey())

  // Fetch labs on mount
  useEffect(() => {
    if (!token) {
      dispatch({ type: 'set_error', message: 'No API token found' })
      return
    }

    dispatch({ type: 'set_loading' })

    fetchLabs(token)
      .then((labs) => {
        dispatch({ type: 'set_success', labs })
      })
      .catch((err: Error) => {
        dispatch({ type: 'set_error', message: err.message })
      })
  }, [token])

  // Fetch analytics when selected lab changes
  useEffect(() => {
    if (!state.selectedLab || !token) return

    // Fetch scores
    fetchScores(token, state.selectedLab)
      .then((scores) => {
        dispatch({ type: 'set_scores', scores })
      })
      .catch((err: Error) => {
        console.error('Error fetching scores:', err)
      })

    // Fetch timeline
    fetchTimeline(token, state.selectedLab)
      .then((timeline) => {
        dispatch({ type: 'set_timeline', timeline })
      })
      .catch((err: Error) => {
        console.error('Error fetching timeline:', err)
      })

    // Fetch pass rates
    fetchPassRates(token, state.selectedLab)
      .then((passRates) => {
        dispatch({ type: 'set_pass_rates', passRates })
      })
      .catch((err: Error) => {
        console.error('Error fetching pass rates:', err)
      })
  }, [state.selectedLab, token])

  function handleLabChange(e: React.ChangeEvent<HTMLSelectElement>) {
    dispatch({ type: 'set_selected_lab', lab: e.target.value })
  }

  if (state.status === 'loading') {
    return <div className="dashboard-loading">Loading dashboard...</div>
  }

  if (state.status === 'error') {
    return (
      <div className="dashboard-error">
        <h2>Error</h2>
        <p>{state.errorMessage}</p>
      </div>
    )
  }

  if (state.labs.length === 0) {
    return (
      <div className="dashboard-empty">
        <h2>No Labs Found</h2>
        <p>There are no labs available in the system.</p>
      </div>
    )
  }

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>Analytics Dashboard</h1>
        <div className="lab-selector">
          <label htmlFor="lab-select">Select Lab: </label>
          <select
            id="lab-select"
            value={state.selectedLab}
            onChange={handleLabChange}
          >
            {state.labs.map((lab) => (
              <option key={lab.id} value={lab.title}>
                {lab.title}
              </option>
            ))}
          </select>
        </div>
      </header>

      <div className="dashboard-content">
        {/* Score Distribution Chart */}
        <section className="dashboard-section">
          <h2>Score Distribution</h2>
          {state.scores.length > 0 ? (
            <Bar data={buildScoreChartData(state.scores)} options={chartOptions} />
          ) : (
            <p className="no-data">No score data available</p>
          )}
        </section>

        {/* Timeline Chart */}
        <section className="dashboard-section">
          <h2>Submissions Timeline</h2>
          {state.timeline.length > 0 ? (
            <Line data={buildTimelineData(state.timeline)} options={chartOptions} />
          ) : (
            <p className="no-data">No timeline data available</p>
          )}
        </section>

        {/* Pass Rates Table */}
        <section className="dashboard-section">
          <h2>Pass Rates by Task</h2>
          {state.passRates.length > 0 ? (
            <table className="pass-rates-table">
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Average Score</th>
                  <th>Attempts</th>
                </tr>
              </thead>
              <tbody>
                {state.passRates.map((entry) => (
                  <tr key={entry.task}>
                    <td>{entry.task}</td>
                    <td>{entry.avg_score.toFixed(1)}</td>
                    <td>{entry.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <p className="no-data">No pass rate data available</p>
          )}
        </section>
      </div>
    </div>
  )
}
