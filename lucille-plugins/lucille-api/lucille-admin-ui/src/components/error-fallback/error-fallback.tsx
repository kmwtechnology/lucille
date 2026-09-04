import { Component } from "react"
import type { ErrorInfo, ReactNode } from "react"
import { Button } from "@/components/ui/button/button"
import { AlertTriangle } from "lucide-react"
import styles from "./error-fallback.module.css"

interface Props {
  children: ReactNode
}

interface State {
  hasError: boolean
  error: Error | null
}

export class ErrorFallback extends Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error("Uncaught error:", error, errorInfo)
  }

  handleReset = () => {
    this.setState({ hasError: false, error: null })
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className={styles.container}>
          <div className={styles.content}>
            <AlertTriangle className={styles.icon} />
            <h1 className={styles.title}>Something went wrong</h1>
            <p className={styles.message}>
              An unexpected error occurred. You can try again or return to the dashboard.
            </p>
            {this.state.error && (
              <pre className={styles.details}>
                {this.state.error.message}
              </pre>
            )}
            <div className={styles.actions}>
              <Button variant="default" onClick={this.handleReset}>
                Try Again
              </Button>
              <Button
                variant="outline"
                onClick={() => {
                  this.handleReset()
                  window.location.href = "/admin"
                }}
              >
                Go to Dashboard
              </Button>
            </div>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}
