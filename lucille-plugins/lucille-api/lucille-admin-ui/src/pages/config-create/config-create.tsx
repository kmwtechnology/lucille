import { useState } from "react"
import { useNavigate } from "react-router-dom"
import { Link } from "@/components/ui/link/link"
import { Button } from "@/components/ui/button/button"
import { Card, CardContent } from "@/components/ui/card/card"
import { Save, Settings } from "lucide-react"
import { BackLink } from "@/components/back-link/back-link"
import { useCreateConfig } from "@/hooks/use-create-config"
import { validateConfigText } from "@/lib/validate-config"
import styles from "./config-create.module.css"

export default function ConfigCreate() {
  const navigate = useNavigate()
  const { state, submit } = useCreateConfig()
  const [text, setText] = useState("")
  const [touched, setTouched] = useState(false)

  const submitting = state.status === "submitting"
  const validation = validateConfigText(text)

  const handleSubmit = async () => {
    if (!validation.valid) {
      setTouched(true)
      return
    }
    const configId = await submit(text)
    if (configId) {
      navigate(`/configs/detail?id=${encodeURIComponent(configId)}`)
    }
  }

  return (
    <div className={styles.page}>
      <BackLink to="/configs" label="Back to Configurations" />

      <div className={styles.header}>
        <div className={styles.titleRow}>
          <Settings className="h-6 w-6 text-primary-600 flex-shrink-0" />
          <h1 className={styles.title}>Create Configuration</h1>
        </div>
        <p className={styles.subtext}>
          Enter your Lucille configuration as HOCON (or JSON). It will be validated and saved
          with a generated ID.
        </p>
      </div>

      <Card className={styles.card}>
        <CardContent className={styles.formContent}>
          <label htmlFor="config-hocon" className={styles.formLabel}>
            Configuration
          </label>
          <textarea
            id="config-hocon"
            className={styles.editor}
            spellCheck={false}
            value={text}
            onChange={(e) => setText(e.target.value)}
            onBlur={() => setTouched(true)}
            disabled={submitting}
          />

          {touched && !validation.valid && (
            <div className={styles.formError}>{validation.message}</div>
          )}
          {state.status === "error" && (
            <div className={styles.formError}>{state.error}</div>
          )}

          <div className={styles.formActions}>
            <Button
              variant="default"
              onClick={handleSubmit}
              disabled={submitting || !validation.valid}
            >
              <Save className="mr-2 h-4 w-4" />
              {submitting ? "Posting..." : "Post Configuration"}
            </Button>
            <Link variant="outline" size="default" to="/configs">
              Cancel
            </Link>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
